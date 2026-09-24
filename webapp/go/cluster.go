package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
)

type clusterHost struct {
	Name string `json:"name"`
	IP   string `json:"ip"`
}

type clusterTopology struct {
	Hosts  []clusterHost `json:"hosts"`
	Self   int           `json:"-"`
	client *http.Client
}

func loadCluster(path string) (*clusterTopology, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	c := new(clusterTopology)
	if err := json.Unmarshal(data, c); err != nil {
		return nil, err
	}
	if len(c.Hosts) != 5 {
		return nil, fmt.Errorf("invalid cluster topology")
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	c.Self = -1
	for i, host := range c.Hosts {
		if host.Name != fmt.Sprintf("isucon-%d", i+1) || net.ParseIP(host.IP) == nil {
			return nil, fmt.Errorf("invalid cluster host %d", i+1)
		}
		for _, addr := range addrs {
			ip, _, err := net.ParseCIDR(addr.String())
			if err == nil && ip.Equal(net.ParseIP(host.IP)) {
				if c.Self != -1 {
					return nil, fmt.Errorf("multiple cluster addresses on this host")
				}
				c.Self = i
			}
		}
	}
	if c.Self < 0 {
		return nil, fmt.Errorf("local address is not in cluster topology")
	}
	c.client = &http.Client{Timeout: 60 * time.Second, Transport: &http.Transport{
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 128,
		IdleConnTimeout:     60 * time.Second,
	}}
	return c, nil
}

func (c *clusterTopology) owner(userID int64) int {
	return int((userID - 1) % int64(len(c.Hosts)))
}

func (c *clusterTopology) routeMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(ctx echo.Context) error {
		path := ctx.Path()
		var userID int64
		var err error
		switch {
		case strings.HasPrefix(path, "/user/:userID/"), strings.HasPrefix(path, "/admin/user/:userID"):
			userID, err = strconv.ParseInt(ctx.Param("userID"), 10, 64)
		case path == "/login":
			body, readErr := io.ReadAll(ctx.Request().Body)
			if readErr != nil {
				return errorResponse(ctx, http.StatusBadRequest, ErrInvalidRequestBody)
			}
			ctx.Request().Body = io.NopCloser(bytes.NewReader(body))
			var req LoginRequest
			err = json.Unmarshal(body, &req)
			userID = req.UserID
		default:
			return next(ctx)
		}
		if err != nil || userID <= 0 {
			return next(ctx)
		}
		owner := c.owner(userID)
		if owner == c.Self {
			return next(ctx)
		}
		if ctx.Request().Header.Get("X-Isu-Internal-Hop") != "" {
			return errorResponse(ctx, http.StatusBadGateway, fmt.Errorf("routing loop"))
		}
		return c.forward(ctx, owner)
	}
}

func (h *Handler) initializeLocalHTTP(ctx echo.Context) error {
	if h.Cluster.Self == 0 {
		return ctx.NoContent(http.StatusForbidden)
	}
	peer, _, err := net.SplitHostPort(ctx.Request().RemoteAddr)
	if err != nil || peer != h.Cluster.Hosts[0].IP {
		return ctx.NoContent(http.StatusForbidden)
	}
	if err := h.resetLocal(ctx.Request().Context()); err != nil {
		return errorResponse(ctx, http.StatusInternalServerError, err)
	}
	return ctx.NoContent(http.StatusNoContent)
}

func (h *Handler) initializeCluster(ctx echo.Context) error {
	if h.Cluster.Self != 0 {
		return ctx.NoContent(http.StatusForbidden)
	}
	resetCtx, cancel := context.WithTimeout(ctx.Request().Context(), 58*time.Second)
	defer cancel()
	if err := h.Cluster.waitForWorkers(resetCtx); err != nil {
		return errorResponse(ctx, http.StatusInternalServerError, err)
	}
	results := make(chan error, len(h.Cluster.Hosts))
	go func() { results <- h.resetLocal(resetCtx) }()
	for i := 1; i < len(h.Cluster.Hosts); i++ {
		i := i
		go func() {
			target := "http://" + net.JoinHostPort(h.Cluster.Hosts[i].IP, "8080") + "/_internal/initialize"
			req, err := http.NewRequestWithContext(resetCtx, http.MethodPost, target, nil)
			if err != nil {
				results <- err
				return
			}
			client := &http.Client{Transport: h.Cluster.client.Transport}
			resp, err := client.Do(req)
			if err != nil {
				results <- err
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusNoContent {
				body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
				results <- fmt.Errorf("%s initialize: %d %s", h.Cluster.Hosts[i].Name, resp.StatusCode, body)
				return
			}
			results <- nil
		}()
	}
	var failed error
	for range h.Cluster.Hosts {
		if err := <-results; err != nil && failed == nil {
			failed = err
		}
	}
	if failed != nil {
		return errorResponse(ctx, http.StatusInternalServerError, failed)
	}
	return successResponse(ctx, &InitializeResponse{Language: "go"})
}

func (c *clusterTopology) waitForWorkers(ctx context.Context) error {
	readyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	client := &http.Client{Timeout: time.Second, Transport: c.client.Transport}
	for i := 1; i < len(c.Hosts); i++ {
		target := "http://" + net.JoinHostPort(c.Hosts[i].IP, "8080") + "/health"
		for {
			req, err := http.NewRequestWithContext(readyCtx, http.MethodGet, target, nil)
			if err != nil {
				return err
			}
			resp, err := client.Do(req)
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					break
				}
				err = fmt.Errorf("health status %d", resp.StatusCode)
			}
			select {
			case <-readyCtx.Done():
				return fmt.Errorf("%s is not ready: %w", c.Hosts[i].Name, err)
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	return nil
}

func (c *clusterTopology) forward(ctx echo.Context, owner int) error {
	original := ctx.Request()
	target := "http://" + net.JoinHostPort(c.Hosts[owner].IP, "8080") + original.URL.RequestURI()
	req, err := http.NewRequestWithContext(original.Context(), original.Method, target, original.Body)
	if err != nil {
		return errorResponse(ctx, http.StatusBadGateway, err)
	}
	req.Header = original.Header.Clone()
	stripHopHeaders(req.Header)
	req.Header.Set("X-Isu-Internal-Hop", "1")
	req.Host = original.Host
	resp, err := c.client.Do(req)
	if err != nil {
		return errorResponse(ctx, http.StatusBadGateway, err)
	}
	defer resp.Body.Close()
	stripHopHeaders(resp.Header)
	for name, values := range resp.Header {
		for _, value := range values {
			ctx.Response().Header().Add(name, value)
		}
	}
	ctx.Response().WriteHeader(resp.StatusCode)
	_, err = io.Copy(ctx.Response(), resp.Body)
	return err
}

func stripHopHeaders(h http.Header) {
	for _, name := range strings.Split(h.Get("Connection"), ",") {
		h.Del(strings.TrimSpace(name))
	}
	for _, name := range []string{"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "TE", "Trailer", "Transfer-Encoding", "Upgrade"} {
		h.Del(name)
	}
}
