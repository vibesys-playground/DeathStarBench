package podctl

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Mode says which pods of each Deployment a restart replaces.
type Mode string

const (
	// One replaces one pod of each Deployment (the first by name); the other
	// replicas keep serving. A Deployment with one replica loses it.
	One Mode = "one"
	// All replaces every pod of each Deployment at once.
	All Mode = "all"
)

// ParseMode accepts "one" and "all".
func ParseMode(value string) (Mode, error) {
	switch Mode(value) {
	case One, All:
		return Mode(value), nil
	}
	return "", fmt.Errorf("restart mode must be %q or %q, got %q", One, All, value)
}

// Controller restarts the Deployments of one namespace and waits until they
// serve.
type Controller struct {
	Kube *Kube
	// Deployments are the ones Ready and Restart act on.
	Deployments []string
	// Registered Deployments register every pod in consul (at its pod IP)
	// before it serves gRPC; their pods serve once ConsulURL lists the pod.
	Registered []string
	// RegisteredAuto treats a Deployment as Registered when any of its pods is
	// listed in consul: by Ready, at each check; by Restart, before it deletes
	// pods, so that replacements must register as the pods they replace did.
	// A Deployment none of whose pods registers is not required to (a merged
	// service registers only the names it serves, or none when nothing calls it).
	RegisteredAuto bool
	ConsulURL      string
	// Probed Deployments serve HTTP on ProbePort; their pods serve once
	// every ProbePaths answers 200 at the pod's IP.
	Probed     []string
	ProbePort  int
	ProbePaths []string
	// Grace is the deleted pods' termination grace period (seconds).
	Grace int
	// Poll is the interval between checks; Timeout bounds a whole call.
	Poll    time.Duration
	Timeout time.Duration
	// Log receives one line per event; nil discards them.
	Log io.Writer
	// HTTP sends the probes and consul queries.
	HTTP *http.Client
	// ProbeURL builds a probe's URL (tests point it at a local server).
	ProbeURL func(ip string, port int, path string) string
}

func (c *Controller) logf(format string, args ...any) {
	if c.Log != nil {
		fmt.Fprintf(c.Log, "podctl: "+format+"\n", args...)
	}
}

func (c *Controller) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return &http.Client{Timeout: 5 * time.Second}
}

func (c *Controller) probeURL(ip string, port int, path string) string {
	if c.ProbeURL != nil {
		return c.ProbeURL(ip, port, path)
	}
	return "http://" + joinHostPort(ip, strconv.Itoa(port)) + path
}

func contains(list []string, item string) bool {
	for _, value := range list {
		if value == item {
			return true
		}
	}
	return false
}

func (c *Controller) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.Timeout > 0 {
		return context.WithTimeout(ctx, c.Timeout)
	}
	return context.WithCancel(ctx)
}

// Ready waits until every Deployment serves (see Serving).
func (c *Controller) Ready(ctx context.Context) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	started := time.Now()
	if err := c.waitAll(ctx, nil, nil); err != nil {
		return err
	}
	c.logf("ready after %.1fs", time.Since(started).Seconds())
	return nil
}

// Restart deletes one pod (One) or every pod (All) of each Deployment, all
// Deployments at once, and returns when no deleted pod remains and every
// Deployment serves with all its replicas again.
func (c *Controller) Restart(ctx context.Context, mode Mode) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	started := time.Now()
	replaced := map[string]map[string]bool{} // deployment -> deleted pod UIDs
	registering := map[string]bool{}         // deployment -> its pods registered before
	var addresses map[string]bool
	if c.RegisteredAuto {
		var err error
		if addresses, err = c.consulAddresses(ctx); err != nil {
			return err
		}
	}
	for _, name := range c.Deployments {
		deployment, err := c.Kube.GetDeployment(ctx, name)
		if err != nil {
			return err
		}
		pods, err := c.Kube.ListPods(ctx, deployment.Selector)
		if err != nil {
			return err
		}
		var victims []Pod
		for _, pod := range pods {
			if pod.Terminating {
				continue
			}
			victims = append(victims, pod)
			if mode == One {
				break
			}
		}
		if len(victims) == 0 {
			return fmt.Errorf("deployment %s has no pod to restart", name)
		}
		for _, pod := range pods {
			if !pod.Terminating && addresses[pod.IP] {
				registering[name] = true
			}
		}
		replaced[name] = map[string]bool{}
		for _, pod := range victims {
			if err := c.Kube.DeletePod(ctx, pod.Name, c.Grace); err != nil {
				return fmt.Errorf("delete pod %s: %w", pod.Name, err)
			}
			replaced[name][pod.UID] = true
			c.logf("restart %s: deleted %s/%s (%s) of %d", mode, name, pod.Name, pod.IP, deployment.Replicas)
		}
	}
	if err := c.waitAll(ctx, replaced, registering); err != nil {
		return err
	}
	c.logf("restart %s: every deployment serves again after %.1fs", mode, time.Since(started).Seconds())
	return nil
}

// waitAll waits for every Deployment concurrently; registering names the
// Deployments whose pods must all be in consul (beyond Registered).
func (c *Controller) waitAll(ctx context.Context, replaced map[string]map[string]bool, registering map[string]bool) error {
	var wg sync.WaitGroup
	errs := make([]error, len(c.Deployments))
	for index, name := range c.Deployments {
		wg.Add(1)
		go func(index int, name string) {
			defer wg.Done()
			errs[index] = c.waitServing(ctx, name, replaced[name], registering[name])
		}(index, name)
	}
	wg.Wait()
	var failed []string
	for _, err := range errs {
		if err != nil {
			failed = append(failed, err.Error())
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("%s", strings.Join(failed, "; "))
	}
	return nil
}

// waitServing polls until Serving reports the Deployment serving.
func (c *Controller) waitServing(ctx context.Context, name string, replaced map[string]bool, registering bool) error {
	poll := c.Poll
	if poll <= 0 {
		poll = 500 * time.Millisecond
	}
	last := "not checked"
	for {
		pods, why, err := c.Serving(ctx, name, replaced, registering)
		if err == nil && why == "" {
			names := make([]string, 0, len(pods))
			for _, pod := range pods {
				names = append(names, pod.Name+" ("+pod.IP+")")
			}
			c.logf("%s serves: %s", name, strings.Join(names, ", "))
			return nil
		}
		switch {
		case ctx.Err() != nil: // keep the reason, not the deadline that cut a request
		case err == nil:
			last = why
		default:
			last = err.Error()
		}
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("%s does not serve: %s", name, last)
		case <-timer.C:
		}
	}
}

// Serving reports whether a Deployment serves: exactly its replica count of
// pods exist, none terminating or in replaced, all Ready; the pods of a
// Registered Deployment (or a registering one, or with RegisteredAuto one with
// any pod in consul) are listed in consul at their IPs; a Probed Deployment's
// pods answer every probe path with 200. It returns the pods, and why it does
// not serve ("" when it does).
func (c *Controller) Serving(ctx context.Context, name string, replaced map[string]bool, registering bool) ([]Pod, string, error) {
	deployment, err := c.Kube.GetDeployment(ctx, name)
	if err != nil {
		return nil, "", err
	}
	pods, err := c.Kube.ListPods(ctx, deployment.Selector)
	if err != nil {
		return nil, "", err
	}
	if len(pods) != deployment.Replicas {
		return pods, fmt.Sprintf("%d pods, want %d", len(pods), deployment.Replicas), nil
	}
	for _, pod := range pods {
		switch {
		case replaced[pod.UID]:
			return pods, fmt.Sprintf("deleted pod %s still exists", pod.Name), nil
		case pod.Terminating:
			return pods, fmt.Sprintf("pod %s is terminating", pod.Name), nil
		case !pod.Ready || pod.IP == "":
			return pods, fmt.Sprintf("pod %s is not ready", pod.Name), nil
		}
	}
	registering = registering || contains(c.Registered, name)
	if registering || c.RegisteredAuto {
		addresses, err := c.consulAddresses(ctx)
		if err != nil {
			return pods, "", err
		}
		for _, pod := range pods {
			registering = registering || addresses[pod.IP] // RegisteredAuto: any pod in consul
		}
		for _, pod := range pods {
			if registering && !addresses[pod.IP] {
				return pods, fmt.Sprintf("pod %s (%s) is not registered in consul", pod.Name, pod.IP), nil
			}
		}
	}
	if contains(c.Probed, name) {
		for _, pod := range pods {
			for _, path := range c.ProbePaths {
				if why := c.probe(ctx, pod, path); why != "" {
					return pods, why, nil
				}
			}
		}
	}
	return pods, "", nil
}

func (c *Controller) probe(ctx context.Context, pod Pod, path string) string {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.probeURL(pod.IP, c.ProbePort, path), nil)
	if err != nil {
		return err.Error()
	}
	response, err := c.httpClient().Do(request)
	if err != nil {
		return fmt.Sprintf("pod %s: GET %s: %v", pod.Name, path, err)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Sprintf("pod %s: GET %s: HTTP %d", pod.Name, path, response.StatusCode)
	}
	return ""
}

// consulAddresses returns the addresses of the services registered with the
// consul agent.
func (c *Controller) consulAddresses(ctx context.Context) (map[string]bool, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.ConsulURL, "/")+"/v1/agent/services", nil)
	if err != nil {
		return nil, err
	}
	response, err := c.httpClient().Do(request)
	if err != nil {
		return nil, fmt.Errorf("consul: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("consul: HTTP %d", response.StatusCode)
	}
	var services map[string]struct {
		Address string `json:"Address"`
	}
	if err := json.NewDecoder(response.Body).Decode(&services); err != nil {
		return nil, fmt.Errorf("consul: %w", err)
	}
	addresses := make(map[string]bool, len(services))
	for _, service := range services {
		addresses[service.Address] = true
	}
	return addresses, nil
}
