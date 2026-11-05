package podctl

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeCluster is a namespace of Deployments behind a fake API server, with
// ReplicaSet behaviour: a deleted pod terminates after a few observations and
// a replacement appears at once, becoming ready (and registered in consul)
// a few observations later.
type fakeCluster struct {
	mu          sync.Mutex
	t           *testing.T
	replicas    map[string]int
	pods        []*fakePod
	next        int
	consul      map[string]bool // registered IPs; never deregistered
	registers   map[string]bool // deployments whose pods register once ready
	noRegister  map[string]bool // deployments whose new pods never register
	badProbe    map[string]bool // pod names that answer probes with 500
	deletes     []string
	graces      []int
	tokenOK     bool
	apiRequests int
}

type fakePod struct {
	name, uid, ip, deployment string
	readyIn                   int // observations until ready
	terminating               int // observations until gone (0: not terminating)
}

func newFakeCluster(t *testing.T, replicas map[string]int) *fakeCluster {
	return newFakeClusterOf(t, replicas, []string{"frontend", "geo", "memcached-reserve"}, map[string]bool{"geo": true})
}

// newFakeClusterOf starts the deployments in order, every pod ready, the
// pods of the registers deployments in consul.
func newFakeClusterOf(t *testing.T, replicas map[string]int, order []string, registers map[string]bool) *fakeCluster {
	c := &fakeCluster{t: t, replicas: replicas, consul: map[string]bool{}, registers: registers, noRegister: map[string]bool{}, badProbe: map[string]bool{}, tokenOK: true}
	for _, name := range order {
		for i := 0; i < replicas[name]; i++ {
			pod := c.addPod(name)
			pod.readyIn = 0
			c.consul[pod.ip] = registers[name]
		}
	}
	return c
}

func (c *fakeCluster) addPod(deployment string) *fakePod {
	c.next++
	pod := &fakePod{
		name: fmt.Sprintf("%s-%04d", deployment, c.next), uid: fmt.Sprintf("uid-%d", c.next),
		ip: fmt.Sprintf("10.0.0.%d", c.next), deployment: deployment, readyIn: 2,
	}
	c.pods = append(c.pods, pod)
	return pod
}

// tick advances the pods' lifecycle by one observation.
func (c *fakeCluster) tick() {
	kept := c.pods[:0]
	for _, pod := range c.pods {
		if pod.terminating > 0 {
			pod.terminating--
			if pod.terminating == 0 {
				continue
			}
		}
		if pod.readyIn > 0 {
			pod.readyIn--
			if pod.readyIn == 0 && c.registers[pod.deployment] && !c.noRegister[pod.deployment] {
				c.consul[pod.ip] = true
			}
		}
		kept = append(kept, pod)
	}
	c.pods = kept
}

func (c *fakeCluster) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if strings.HasPrefix(r.URL.Path, "/probe/") {
		ip := strings.TrimPrefix(r.URL.Path, "/probe/")
		for _, pod := range c.pods {
			if pod.ip == ip && pod.readyIn == 0 && pod.terminating == 0 && !c.badProbe[pod.name] {
				w.WriteHeader(http.StatusOK)
				return
			}
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if r.URL.Path == "/v1/agent/services" {
		services := map[string]map[string]string{}
		for ip, registered := range c.consul {
			if registered {
				services["id-"+ip] = map[string]string{"Service": "srv", "Address": ip}
			}
		}
		json.NewEncoder(w).Encode(services)
		return
	}
	c.apiRequests++
	if r.Header.Get("Authorization") != "Bearer secret" {
		c.tokenOK = false
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	switch {
	case r.Method == http.MethodGet && len(parts) == 7 && parts[0] == "apis" && parts[5] == "deployments":
		replicas, ok := c.replicas[parts[6]]
		if !ok {
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"spec": map[string]any{
			"replicas": replicas,
			"selector": map[string]any{"matchLabels": map[string]string{"app": parts[6], "tier": "app"}},
		}})
	case r.Method == http.MethodGet && len(parts) == 5 && parts[4] == "pods":
		c.tick()
		selector := r.URL.Query().Get("labelSelector")
		var deployment string
		for _, term := range strings.Split(selector, ",") {
			if key, value, _ := strings.Cut(term, "="); key == "app" {
				deployment = value
			}
		}
		if selector != "app="+deployment+",tier=app" {
			c.t.Errorf("label selector %q", selector)
		}
		items := []any{}
		for _, pod := range c.pods {
			if pod.deployment != deployment {
				continue
			}
			metadata := map[string]any{"name": pod.name, "uid": pod.uid}
			if pod.terminating > 0 {
				metadata["deletionTimestamp"] = "2026-11-04T00:00:00Z"
			}
			ready := "False"
			if pod.readyIn == 0 {
				ready = "True"
			}
			items = append(items, map[string]any{"metadata": metadata, "status": map[string]any{
				"phase": "Running", "podIP": pod.ip,
				"conditions": []any{map[string]string{"type": "Ready", "status": ready}},
			}})
		}
		json.NewEncoder(w).Encode(map[string]any{"items": items})
	case r.Method == http.MethodDelete && len(parts) == 6 && parts[4] == "pods":
		var options struct {
			GracePeriodSeconds int `json:"gracePeriodSeconds"`
		}
		json.NewDecoder(r.Body).Decode(&options)
		for _, pod := range c.pods {
			if pod.name == parts[5] && pod.terminating == 0 {
				pod.terminating = 2
				c.deletes = append(c.deletes, pod.name)
				c.graces = append(c.graces, options.GracePeriodSeconds)
				c.addPod(pod.deployment)
				w.Write([]byte("{}"))
				return
			}
		}
		http.NotFound(w, r)
	default:
		c.t.Errorf("unexpected %s %s", r.Method, r.URL)
		http.NotFound(w, r)
	}
}

func (c *fakeCluster) live(deployment string) map[string]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	uids := map[string]bool{}
	for _, pod := range c.pods {
		if pod.deployment == deployment {
			uids[pod.uid] = true
		}
	}
	return uids
}

func newController(t *testing.T, cluster *fakeCluster) *Controller {
	server := httptest.NewServer(cluster)
	t.Cleanup(server.Close)
	return &Controller{
		Kube:        &Kube{BaseURL: server.URL, Namespace: "hk8s-test", Token: "secret", HTTP: server.Client()},
		Deployments: []string{"frontend", "geo", "memcached-reserve"},
		Registered:  []string{"geo"}, ConsulURL: server.URL,
		Probed: []string{"frontend"}, ProbePort: 5000,
		ProbePaths: []string{"/hotels?inDate=2300-01-01&outDate=2300-01-02", "/user?username=u&password=p"},
		Grace:      1, Poll: time.Millisecond, Timeout: 5 * time.Second,
		ProbeURL: func(ip string, port int, path string) string {
			if port != 5000 {
				t.Errorf("probe port %d", port)
			}
			if _, err := url.Parse(path); err != nil {
				t.Errorf("probe path %q: %v", path, err)
			}
			return server.URL + "/probe/" + ip
		},
	}
}

func TestRestartOneReplacesOnePodOfEachDeployment(t *testing.T) {
	cluster := newFakeCluster(t, map[string]int{"frontend": 2, "geo": 2, "memcached-reserve": 1})
	controller := newController(t, cluster)
	before := map[string]map[string]bool{}
	for _, name := range controller.Deployments {
		before[name] = cluster.live(name)
	}
	if err := controller.Restart(context.Background(), One); err != nil {
		t.Fatal(err)
	}
	if len(cluster.deletes) != 3 {
		t.Fatalf("deleted %v, want one pod of each of 3 deployments", cluster.deletes)
	}
	for _, grace := range cluster.graces {
		if grace != 1 {
			t.Fatalf("grace periods %v", cluster.graces)
		}
	}
	for name, replicas := range map[string]int{"frontend": 2, "geo": 2, "memcached-reserve": 1} {
		after := cluster.live(name)
		kept := 0
		for uid := range after {
			if before[name][uid] {
				kept++
			}
		}
		if len(after) != replicas || kept != replicas-1 {
			t.Fatalf("%s: %d pods after, %d of them from before; want %d and %d", name, len(after), kept, replicas, replicas-1)
		}
	}
	if !cluster.tokenOK {
		t.Fatal("a request lacked the service account token")
	}
}

func TestRestartAllReplacesEveryPod(t *testing.T) {
	cluster := newFakeCluster(t, map[string]int{"frontend": 2, "geo": 2, "memcached-reserve": 1})
	controller := newController(t, cluster)
	before := cluster.live("frontend")
	if err := controller.Restart(context.Background(), All); err != nil {
		t.Fatal(err)
	}
	if len(cluster.deletes) != 5 {
		t.Fatalf("deleted %v, want all 5 pods", cluster.deletes)
	}
	for uid := range cluster.live("frontend") {
		if before[uid] {
			t.Fatalf("frontend pod %s survived a restart of all pods", uid)
		}
	}
}

func TestWaitsForEveryFrontendPodAndConsul(t *testing.T) {
	cluster := newFakeCluster(t, map[string]int{"frontend": 2, "geo": 2, "memcached-reserve": 1})
	controller := newController(t, cluster)
	controller.Timeout = time.Second
	set := func(change func()) {
		cluster.mu.Lock()
		defer cluster.mu.Unlock()
		change()
	}
	second := cluster.pods[1].name // the second frontend pod
	set(func() { cluster.badProbe[second] = true })
	err := controller.Ready(context.Background())
	if err == nil || !strings.Contains(err.Error(), second) {
		t.Fatalf("a frontend pod failing its probes: %v", err)
	}
	set(func() { delete(cluster.badProbe, second) })
	if err := controller.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	set(func() { cluster.noRegister["geo"] = true })
	err = controller.Restart(context.Background(), One)
	if err == nil || !strings.Contains(err.Error(), "not registered in consul") {
		t.Fatalf("a geo pod that never registers: %v", err)
	}
}

func TestParseModeAndSelector(t *testing.T) {
	for _, value := range []string{"one", "all"} {
		if mode, err := ParseMode(value); err != nil || string(mode) != value {
			t.Fatalf("%s: %v %v", value, mode, err)
		}
	}
	if _, err := ParseMode("some"); err == nil {
		t.Fatal("accepted mode some")
	}
	if got := LabelSelector(map[string]string{"b": "2", "a": "1"}); got != "a=1,b=2" {
		t.Fatalf("selector %q", got)
	}
}

// The candidate's own Deployments: merged groups with any names and replica
// counts, consul registration inferred (--registered-auto).
func autoController(t *testing.T, cluster *fakeCluster, deployments []string) *Controller {
	controller := newController(t, cluster)
	controller.Deployments = deployments
	controller.Registered = nil
	controller.RegisteredAuto = true
	controller.Probed = []string{"web"}
	return controller
}

func TestRestartOneOfEachCandidateDeployment(t *testing.T) {
	replicas := map[string]int{"web": 3, "search-geo-rate": 2, "profile": 4, "memcached-reserve": 1}
	order := []string{"web", "search-geo-rate", "profile", "memcached-reserve"}
	cluster := newFakeClusterOf(t, replicas, order, map[string]bool{"search-geo-rate": true, "profile": true})
	controller := autoController(t, cluster, order)
	before := map[string]map[string]bool{}
	for _, name := range order {
		before[name] = cluster.live(name)
	}
	if err := controller.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := controller.Restart(context.Background(), One); err != nil {
		t.Fatal(err)
	}
	deleted := map[string]int{}
	for _, name := range cluster.deletes {
		deleted[name[:strings.LastIndex(name, "-")]]++
	}
	for _, name := range order {
		kept := 0
		for uid := range cluster.live(name) {
			if before[name][uid] {
				kept++
			}
		}
		if deleted[name] != 1 || kept != replicas[name]-1 {
			t.Fatalf("%s: deleted %d, kept %d of %d; want one replaced while the rest serve (deletes %v)",
				name, deleted[name], kept, replicas[name], cluster.deletes)
		}
	}
	if err := controller.Restart(context.Background(), All); err != nil {
		t.Fatal(err)
	}
	if len(cluster.deletes) != 4+(3+2+4+1) { // one of each, then all 10
		t.Fatalf("restart all deleted %d pods in total, want every pod: %v", len(cluster.deletes), cluster.deletes)
	}
}

func TestRegisteredAutoRequiresEveryPodOfARegisteringDeployment(t *testing.T) {
	replicas := map[string]int{"web": 2, "search-geo-rate": 2}
	order := []string{"web", "search-geo-rate"}
	cluster := newFakeClusterOf(t, replicas, order, map[string]bool{"search-geo-rate": true})
	controller := autoController(t, cluster, order)
	controller.Timeout = time.Second
	// web never registers and is not required to; search-geo-rate is.
	if err := controller.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	// One pod of a registering Deployment missing from consul: not ready.
	cluster.mu.Lock()
	var second string
	for _, pod := range cluster.pods {
		if pod.deployment == "search-geo-rate" {
			second = pod.name
			cluster.consul[pod.ip] = false
			break
		}
	}
	cluster.mu.Unlock()
	if err := controller.Ready(context.Background()); err == nil || !strings.Contains(err.Error(), second) {
		t.Fatalf("a registering Deployment with a pod missing from consul: %v", err)
	}
	// After restarting every pod, the replacements of a Deployment that
	// registered must register too, even though no pod of it is in consul.
	cluster.mu.Lock()
	for _, pod := range cluster.pods {
		cluster.consul[pod.ip] = cluster.registers[pod.deployment]
	}
	cluster.noRegister["search-geo-rate"] = true
	cluster.mu.Unlock()
	err := controller.Restart(context.Background(), All)
	if err == nil || !strings.Contains(err.Error(), "search-geo-rate does not serve") || !strings.Contains(err.Error(), "not registered in consul") {
		t.Fatalf("replacements that never register: %v", err)
	}
	if strings.Contains(err.Error(), "web does not serve") {
		t.Fatalf("web never registered, so its replacements need not: %v", err)
	}
}
