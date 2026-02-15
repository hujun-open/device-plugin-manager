package dpm

import (
	"iter"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/golang/glog"

	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

// TODO: Make plugin server start retries configurable.
const (
	startPluginServerRetries   = 3
	startPluginServerRetryWait = 3 * time.Second
)

// Manager contains the main machinery of this framework. It uses user defined lister to monitor
// available resources and start/stop plugins accordingly. It also handles system signals and
// unexpected kubelet events.
type Manager struct {
	lister       ListerInterface
	pluginMap    map[string]devicePlugin
	pluginMapMux *sync.RWMutex
}

// NewManager is the canonical way of initializing Manager. User must provide ListerInterface
// implementation. Lister will provide information about handled resources, monitor their
// availability and provide method to spawn plugins that will handle found resources.
func NewManager(lister ListerInterface) *Manager {
	dpm := &Manager{
		lister:       lister,
		pluginMap:    make(map[string]devicePlugin),
		pluginMapMux: new(sync.RWMutex),
	}
	return dpm
}

// Run starts the Manager. It sets up the infrastructure and handles system signals, Kubelet socket
// watch and monitoring of available resources as well as starting and stoping of plugins.
func (dpm *Manager) Run() {
	glog.V(3).Info("Starting device plugin manager")

	// First important signal channel is the os signal channel. We only care about (somewhat) small
	// subset of available signals.
	glog.V(3).Info("Registering for system signal notifications")
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT)

	// The other important channel is filesystem notification channel, responsible for watching
	// device plugin directory.
	glog.V(3).Info("Registering for notifications of filesystem changes in device plugin directory")
	fsWatcher, _ := fsnotify.NewWatcher()
	defer fsWatcher.Close()
	fsWatcher.Add(pluginapi.DevicePluginPath)

	// Create list of running plugins and start Discover method of given lister. This method is
	// responsible of notifying manager about changes in available plugins.

	glog.V(3).Info("Starting Discovery on new plugins")
	pluginsCh := make(chan PluginNameList)
	defer close(pluginsCh)
	go dpm.lister.Discover(pluginsCh)

	// Finally start a loop that will handle messages from opened channels.
	glog.V(3).Info("Handling incoming signals")
HandleSignals:
	for {
		select {
		case newPluginsList := <-pluginsCh:
			glog.V(3).Infof("Received new list of plugins: %s", newPluginsList)
			dpm.handleNewPlugins(newPluginsList)
		case event := <-fsWatcher.Events:
			if event.Name == pluginapi.KubeletSocket {
				glog.V(3).Infof("Received kubelet socket event: %s", event)
				if event.Op&fsnotify.Create == fsnotify.Create {
					dpm.startPluginServers()
				}
				// TODO: Kubelet doesn't really clean-up it's socket, so this is currently
				// manual-testing thing. Could we solve Kubelet deaths better?
				if event.Op&fsnotify.Remove == fsnotify.Remove {
					dpm.stopPluginServers()
				}
			}
		case s := <-signalCh:
			switch s {
			case syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT:
				glog.V(3).Infof("Received signal \"%v\", shutting down", s)
				dpm.stopPlugins()
				break HandleSignals
			}
		}
	}
}

func (dpm *Manager) getDevicePlugin(name string) (devicePlugin, bool) {
	dpm.pluginMapMux.RLock()
	defer dpm.pluginMapMux.RUnlock()
	r, ok := dpm.pluginMap[name]
	return r, ok
}

func (dpm *Manager) setDevicePlugin(name string, dp devicePlugin) {
	dpm.pluginMapMux.Lock()
	defer dpm.pluginMapMux.Unlock()
	dpm.pluginMap[name] = dp
}

func (dpm *Manager) delDevicePlugin(name string) {
	dpm.pluginMapMux.Lock()
	defer dpm.pluginMapMux.Unlock()
	delete(dpm.pluginMap, name)
}

func (dpm *Manager) iterPluginMap() iter.Seq2[string, devicePlugin] {
	return func(yield func(string, devicePlugin) bool) {
		dpm.pluginMapMux.RLock()
		defer dpm.pluginMapMux.RUnlock() // Guaranteed to unlock even on 'break'

		for k, v := range dpm.pluginMap {
			if !yield(k, v) {
				return // The caller stopped the loop (e.g., via 'break')
			}
		}
	}
}

func (dpm *Manager) handleNewPlugins(newPluginsList PluginNameList) {
	var wg sync.WaitGroup

	// This map is used for faster searches when removing old plugins
	newPluginsSet := make(map[string]bool)

	// Add new plugins
	for _, newPluginLastName := range newPluginsList {
		newPluginsSet[newPluginLastName] = true
		wg.Add(1)
		go func(name string) {
			if _, ok := dpm.getDevicePlugin(name); !ok {
				// add new plugin only if it doesn't already exist
				glog.V(3).Infof("Adding a new plugin \"%s\"", name)
				plugin := newDevicePlugin(dpm.lister.GetResourceNamespace(), name, dpm.lister.NewPlugin(name))
				startPlugin(name, plugin)
				dpm.setDevicePlugin(name, plugin)
			}
			wg.Done()
		}(newPluginLastName)
	}
	wg.Wait()

	// Remove old plugins
	for pluginLastName, currentPlugin := range dpm.iterPluginMap() {
		wg.Add(1)
		go func(name string, plugin devicePlugin) {
			if _, found := newPluginsSet[name]; !found {
				glog.V(3).Infof("Remove unused plugin \"%s\"", name)
				stopPlugin(name, plugin)
				dpm.delDevicePlugin(name)
			}
			wg.Done()
		}(pluginLastName, currentPlugin)
	}
	wg.Wait()
}

func (dpm *Manager) startPluginServers() {
	var wg sync.WaitGroup

	for pluginLastName, currentPlugin := range dpm.iterPluginMap() {
		wg.Add(1)
		go func(name string, plugin devicePlugin) {
			startPluginServer(name, plugin)
			wg.Done()
		}(pluginLastName, currentPlugin)
	}
	wg.Wait()
}

func (dpm *Manager) stopPluginServers() {
	var wg sync.WaitGroup

	for pluginLastName, currentPlugin := range dpm.iterPluginMap() {
		wg.Add(1)
		go func(name string, plugin devicePlugin) {
			stopPluginServer(name, plugin)
			wg.Done()
		}(pluginLastName, currentPlugin)
	}
	wg.Wait()
}

func (dpm *Manager) stopPlugins() {
	var wg sync.WaitGroup

	for pluginLastName, currentPlugin := range dpm.iterPluginMap() {
		wg.Add(1)
		go func(name string, plugin devicePlugin) {
			stopPlugin(name, plugin)
			dpm.delDevicePlugin(name)
			wg.Done()
		}(pluginLastName, currentPlugin)
	}
	wg.Wait()
}

func startPlugin(pluginLastName string, plugin devicePlugin) {
	var err error
	if devicePluginImpl, ok := plugin.DevicePluginImpl.(PluginInterfaceStart); ok {
		err = devicePluginImpl.Start()
		if err != nil {
			glog.Errorf("Failed to start plugin \"%s\": %s", pluginLastName, err)
		}
	}
	if err == nil {
		startPluginServer(pluginLastName, plugin)
	}
}

func stopPlugin(pluginLastName string, plugin devicePlugin) {
	stopPluginServer(pluginLastName, plugin)
	if devicePluginImpl, ok := plugin.DevicePluginImpl.(PluginInterfaceStop); ok {
		err := devicePluginImpl.Stop()
		if err != nil {
			glog.Errorf("Failed to stop plugin \"%s\": %s", pluginLastName, err)
		}
	}
}

func startPluginServer(pluginLastName string, plugin devicePlugin) {
	for i := 1; i <= startPluginServerRetries; i++ {
		err := plugin.StartServer()
		if err == nil {
			return
		} else if i == startPluginServerRetries {
			glog.V(3).Infof("Failed to start plugin's \"%s\" server, within given %d tries: %s",
				pluginLastName, startPluginServerRetries, err)
		} else {
			glog.Errorf("Failed to start plugin's \"%s\" server, atempt %d ouf of %d waiting %d before next try: %s",
				pluginLastName, i, startPluginServerRetries, startPluginServerRetryWait, err)
			time.Sleep(startPluginServerRetryWait)
		}
	}
}

func stopPluginServer(pluginLastName string, plugin devicePlugin) {
	err := plugin.StopServer()
	if err != nil {
		glog.Errorf("Failed to stop plugin's \"%s\" server: %s", pluginLastName, err)
	}
}
