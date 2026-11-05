// Package podctl restarts and waits for the application's pods from inside
// the job's namespace, through the Kubernetes API with the pod's service
// account (no kubectl in the evaluator image). The oracle's restart hooks and
// the deploy-time readiness wait use it (cmd/podctl).
package podctl

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

const serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// Kube is a minimal client of the Kubernetes API, scoped to one namespace.
type Kube struct {
	BaseURL   string // https://host:port
	Namespace string
	Token     string
	HTTP      *http.Client
}

// InCluster returns a client for the pod's own namespace (or namespace, when
// not empty), authenticated with the pod's service account.
func InCluster(namespace string) (*Kube, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("not in a Kubernetes pod (KUBERNETES_SERVICE_HOST/PORT are not set)")
	}
	token, err := os.ReadFile(serviceAccountDir + "/token")
	if err != nil {
		return nil, fmt.Errorf("service account token: %w", err)
	}
	ca, err := os.ReadFile(serviceAccountDir + "/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("service account CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("service account CA holds no certificate")
	}
	if namespace == "" {
		own, err := os.ReadFile(serviceAccountDir + "/namespace")
		if err != nil {
			return nil, fmt.Errorf("service account namespace: %w", err)
		}
		namespace = strings.TrimSpace(string(own))
	}
	return &Kube{
		BaseURL:   "https://" + joinHostPort(host, port),
		Namespace: namespace,
		Token:     strings.TrimSpace(string(token)),
		HTTP: &http.Client{
			Timeout:   30 * time.Second,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}},
		},
	}, nil
}

func joinHostPort(host, port string) string {
	if strings.Contains(host, ":") {
		return "[" + host + "]:" + port
	}
	return host + ":" + port
}

// Deployment is what podctl needs of a Deployment.
type Deployment struct {
	Name     string
	Replicas int
	Selector map[string]string
}

// Pod is what podctl needs of a pod.
type Pod struct {
	Name        string
	UID         string
	IP          string
	Ready       bool
	Terminating bool
}

func (k *Kube) do(ctx context.Context, method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, k.BaseURL+path, reader)
	if err != nil {
		return err
	}
	if k.Token != "" {
		request.Header.Set("Authorization", "Bearer "+k.Token)
	}
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := k.HTTP.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if response.StatusCode/100 != 2 {
		return &StatusError{Method: method, Path: path, Status: response.StatusCode, Body: strings.TrimSpace(string(raw))}
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(raw, out)
}

// StatusError is a non-2xx answer of the API server.
type StatusError struct {
	Method, Path string
	Status       int
	Body         string
}

func (e *StatusError) Error() string {
	body := e.Body
	if len(body) > 300 {
		body = body[:300]
	}
	return fmt.Sprintf("%s %s: HTTP %d: %s", e.Method, e.Path, e.Status, body)
}

// GetDeployment reads a Deployment's replica count and pod selector.
func (k *Kube) GetDeployment(ctx context.Context, name string) (Deployment, error) {
	var raw struct {
		Spec struct {
			Replicas *int `json:"replicas"`
			Selector struct {
				MatchLabels map[string]string `json:"matchLabels"`
			} `json:"selector"`
		} `json:"spec"`
	}
	path := fmt.Sprintf("/apis/apps/v1/namespaces/%s/deployments/%s", url.PathEscape(k.Namespace), url.PathEscape(name))
	if err := k.do(ctx, http.MethodGet, path, nil, &raw); err != nil {
		return Deployment{}, err
	}
	if len(raw.Spec.Selector.MatchLabels) == 0 {
		return Deployment{}, fmt.Errorf("deployment %s has no matchLabels selector", name)
	}
	replicas := 1
	if raw.Spec.Replicas != nil {
		replicas = *raw.Spec.Replicas
	}
	return Deployment{Name: name, Replicas: replicas, Selector: raw.Spec.Selector.MatchLabels}, nil
}

// ListPods lists the pods that match selector, sorted by name.
func (k *Kube) ListPods(ctx context.Context, selector map[string]string) ([]Pod, error) {
	var raw struct {
		Items []struct {
			Metadata struct {
				Name              string  `json:"name"`
				UID               string  `json:"uid"`
				DeletionTimestamp *string `json:"deletionTimestamp"`
			} `json:"metadata"`
			Status struct {
				Phase      string `json:"phase"`
				PodIP      string `json:"podIP"`
				Conditions []struct {
					Type   string `json:"type"`
					Status string `json:"status"`
				} `json:"conditions"`
			} `json:"status"`
		} `json:"items"`
	}
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods?labelSelector=%s",
		url.PathEscape(k.Namespace), url.QueryEscape(LabelSelector(selector)))
	if err := k.do(ctx, http.MethodGet, path, nil, &raw); err != nil {
		return nil, err
	}
	pods := make([]Pod, 0, len(raw.Items))
	for _, item := range raw.Items {
		pod := Pod{
			Name: item.Metadata.Name, UID: item.Metadata.UID, IP: item.Status.PodIP,
			Terminating: item.Metadata.DeletionTimestamp != nil,
		}
		for _, condition := range item.Status.Conditions {
			if condition.Type == "Ready" {
				pod.Ready = condition.Status == "True" && item.Status.Phase == "Running"
			}
		}
		pods = append(pods, pod)
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })
	return pods, nil
}

// DeletePod deletes a pod with the given grace period. A pod that is
// already gone is not an error.
func (k *Kube) DeletePod(ctx context.Context, name string, graceSeconds int) error {
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s", url.PathEscape(k.Namespace), url.PathEscape(name))
	body := map[string]any{"kind": "DeleteOptions", "apiVersion": "v1", "gracePeriodSeconds": graceSeconds}
	err := k.do(ctx, http.MethodDelete, path, body, nil)
	if status, ok := err.(*StatusError); ok && status.Status == http.StatusNotFound {
		return nil
	}
	return err
}

// LabelSelector renders matchLabels as a label selector, sorted by key.
func LabelSelector(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+labels[key])
	}
	return strings.Join(parts, ",")
}
