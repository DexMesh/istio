// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package consul

import (
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/log"
)

// Controller communicates with Consul and monitors for changes
type Controller struct {
	client           *api.Client
	monitor          Monitor
	services         map[string]*model.Service //key hostname value service
	servicesList     []*model.Service
	serviceInstances map[string][]*model.ServiceInstance //key hostname value serviceInstance array
	mutex            sync.Mutex
}

// NewController creates a new Consul controller
func NewController(addr string, interval time.Duration) (*Controller, error) {
	conf := api.DefaultConfig()
	conf.Address = addr

	client, err := api.NewClient(conf)
	return &Controller{
		monitor:          NewConsulMonitor(client, interval),
		client:           client,
		services:         make(map[string]*model.Service),
		servicesList:     make([]*model.Service, 0),
		serviceInstances: make(map[string][]*model.ServiceInstance),
		mutex:            sync.Mutex{},
	}, err
}

// Services list declarations of all services in the system
func (c *Controller) Services() ([]*model.Service, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(c.services) == 0 {
		c.initServiceCache()
	}
	return c.servicesList, nil
}

func (c *Controller) initServiceCache() {
	c.services = make(map[string]*model.Service)
	c.serviceInstances = make(map[string][]*model.ServiceInstance)
	// get all services from consul
	data, err := c.getServicesFromCounsul()
	if err == nil {
		for name := range data {
			endpoints, err := c.getCatalogService(name, nil)
			if err == nil {
				c.services[name] = convertService(endpoints)
			}
		}
	}

	c.servicesList = make([]*model.Service, 0, len(c.services))

	for _, value := range c.services {
		c.servicesList = append(c.servicesList, value)
	}

	// get all service instances from consul
	for name, _ := range c.services {
		endpoints, err := c.getCatalogService(name, nil)
		if err == nil {
			instances := make([]*model.ServiceInstance, len(endpoints))
			for i, endpoint := range endpoints {
				instances[i] = convertInstance(endpoint)
			}
			c.serviceInstances[name] = instances
		}
	}
}

// GetService retrieves a service by host name if it exists
func (c *Controller) GetService(hostname model.Hostname) (*model.Service, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(c.services) == 0 {
		c.initServiceCache()
	}
	// Get actual service by name
	name, err := parseHostname(hostname)
	if err != nil {
		log.Infof("parseHostname(%s) => error %v", hostname, err)
		return nil, err
	}

	if service, ok := c.services[name]; ok {
		return service, nil
	}
	return nil, fmt.Errorf("Could not find service: %s", name)
}

// GetServiceAttributes retrieves namespace of a service if it exists.
func (c *Controller) GetServiceAttributes(hostname model.Hostname) (*model.ServiceAttributes, error) {
	svc, err := c.GetService(hostname)
	if svc != nil {
		return &model.ServiceAttributes{
			Name:      hostname.String(),
			Namespace: model.IstioDefaultConfigNamespace}, nil
	}
	return nil, err
}

func (c *Controller) getServicesFromCounsul() (map[string][]string, error) {
	data, _, err := c.client.Catalog().Services(nil)
	if err != nil {
		log.Warnf("Could not retrieve services from consul: %v", err)
		return nil, err
	}

	return data, nil
}

func (c *Controller) getCatalogService(name string, q *api.QueryOptions) ([]*api.CatalogService, error) {
	endpoints, _, err := c.client.Catalog().Service(name, "", q)
	if err != nil {
		log.Warnf("Could not retrieve service catalogue from consul: %v", err)
		return nil, err
	}

	return endpoints, nil
}

// ManagementPorts retrieves set of health check ports by instance IP.
// This does not apply to Consul service registry, as Consul does not
// manage the service instances. In future, when we integrate Nomad, we
// might revisit this function.
func (c *Controller) ManagementPorts(addr string) model.PortList {
	return nil
}

// WorkloadHealthCheckInfo retrieves set of health check info by instance IP.
// This does not apply to Consul service registry, as Consul does not
// manage the service instances. In future, when we integrate Nomad, we
// might revisit this function.
func (c *Controller) WorkloadHealthCheckInfo(addr string) model.ProbeList {
	return nil
}

// Instances retrieves instances for a service and its ports that match
// any of the supplied labels. All instances match an empty tag list.
func (c *Controller) Instances(hostname model.Hostname, ports []string,
	labels model.LabelsCollection) ([]*model.ServiceInstance, error) {
	return nil, fmt.Errorf("NOT IMPLEMENTED")
}

// InstancesByPort retrieves instances for a service that match
// any of the supplied labels. All instances match an empty tag list.
func (c *Controller) InstancesByPort(hostname model.Hostname, port int,
	labels model.LabelsCollection) ([]*model.ServiceInstance, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(c.services) == 0 {
		c.initServiceCache()
	}
	// Get actual service by name
	name, err := parseHostname(hostname)
	if err != nil {
		log.Infof("parseHostname(%s) => error %v", hostname, err)
		return nil, err
	}

	if serviceInstances, ok := c.serviceInstances[name]; ok {
		instances := []*model.ServiceInstance{}

		for _, instance := range serviceInstances {
			if labels.HasSubsetOf(instance.Labels) && portMatch(instance, port) {
				instances = append(instances, instance)
			}
		}
		return serviceInstances, nil
	}
	return nil, fmt.Errorf("Could not find instance of service: %s", name)
}

func portMatch(instance *model.ServiceInstance, port int) bool {
	return port == 0 || port == instance.Endpoint.ServicePort.Port
}

// GetProxyServiceInstances lists service instances co-located with a given proxy
func (c *Controller) GetProxyServiceInstances(node *model.Proxy) ([]*model.ServiceInstance, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(c.services) == 0 {
		c.initServiceCache()
	}

	out := make([]*model.ServiceInstance, 0)
	for _, instances := range c.serviceInstances {
		for _, instance := range instances {
			addr := instance.Endpoint.Address
			if len(node.IPAddresses) > 0 {
				for _, ipAddress := range node.IPAddresses {
					if ipAddress == addr {
						out = append(out, instance)
						break
					}
				}
			}
		}
	}

	return out, nil
}

// Run all controllers until a signal is received
func (c *Controller) Run(stop <-chan struct{}) {
	c.monitor.Start(stop)
}

// AppendServiceHandler implements a service catalog operation
func (c *Controller) AppendServiceHandler(f func(*model.Service, model.Event)) error {
	c.monitor.AppendServiceHandler(func(instances []*api.CatalogService, event model.Event) error {
		f(convertService(instances), event)

		c.mutex.Lock()
		defer c.mutex.Unlock()

		// Refresh the service and service instance cache
		c.initServiceCache()
		return nil
	})
	return nil
}

// AppendInstanceHandler implements a service catalog operation
func (c *Controller) AppendInstanceHandler(f func(*model.ServiceInstance, model.Event)) error {
	c.monitor.AppendInstanceHandler(func(instance *api.CatalogService, event model.Event) error {
		f(convertInstance(instance), event)

		c.mutex.Lock()
		defer c.mutex.Unlock()

		// Refresh the service instance cache
		c.initServiceCache()
		return nil
	})
	return nil
}

// GetIstioServiceAccounts implements model.ServiceAccounts operation TODO
func (c *Controller) GetIstioServiceAccounts(hostname model.Hostname, ports []string) []string {
	// Need to get service account of service registered with consul
	// Currently Consul does not have service account or equivalent concept
	// As a step-1, to enabling istio security in Consul, We assume all the services run in default service account
	// This will allow all the consul services to do mTLS
	// Follow - https://goo.gl/Dt11Ct

	return []string{
		"spiffe://cluster.local/ns/default/sa/default",
	}
}
