package core

import (
	"context"
	"encoding/json"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
	"github.com/openkruise/agents/pkg/utils/webhookutils"
)

func enableInjectCsiMountConfig(sandbox *agentsv1alpha1.Sandbox) bool {
	return sandbox.Annotations[agentsv1alpha1.ShouldInjectCsiMount] == "true"
}

func enableInjectAgentRuntimeConfig(sandbox *agentsv1alpha1.Sandbox) bool {
	return sandbox.Annotations[agentsv1alpha1.ShouldInjectAgentRuntime] == "true"
}

// fetchInjectionConfiguration retrieves the sidecar injection configuration from the ConfigMap.
// It attempts to fetch the ConfigMap named sandboxInjectionConfigName from the sandbox-system namespace.
// If the ConfigMap exists, it returns the data map containing configuration keys like
// KEY_CSI_INJECTION_CONFIG and KEY_RUNTIME_INJECTION_CONFIG. If the ConfigMap is not found
// or any error occurs during retrieval, it returns nil data and no error.
//
// Parameters:
//   - ctx: context.Context for the operation
//   - client: Kubernetes client.Interface used to retrieve the ConfigMap
//
// Returns:
//   - map[string]string: The configuration data from the ConfigMap, or nil if not found or on error
//   - error: Always returns nil (errors are logged but not propagated)
func fetchInjectionConfiguration(ctx context.Context, cli client.Client) (map[string]string, error) {
	logger := logf.FromContext(ctx)
	config := &corev1.ConfigMap{}
	err := cli.Get(ctx, types.NamespacedName{
		Namespace: webhookutils.GetNamespace(), // Todo considering the security concern and rbac issue
		Name:      sandboxInjectionConfigName,
	}, config)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Info("injection configuration not found, skip injection")
			return map[string]string{}, nil
		}
		return map[string]string{}, err
	}
	return config.Data, nil
}

// parseInjectConfig parses the sidecar injection configuration from raw ConfigMap data for a specific config key.
// It extracts the value associated with the given configKey (e.g., KEY_CSI_INJECTION_CONFIG or KEY_RUNTIME_INJECTION_CONFIG)
// and unmarshals it into a SidecarInjectConfig struct. If the key is not present in the configRaw map or the value
// is empty, it returns an empty SidecarInjectConfig without error. The configuration includes the main container
// settings, sidecar containers list (CSI sidecars or runtime init containers), and volumes to be injected.
//
// Parameters:
//   - ctx: context.Context for the operation, used for logging
//   - configKey: string representing the configuration key to look up (KEY_CSI_INJECTION_CONFIG or KEY_RUNTIME_INJECTION_CONFIG)
//   - configRaw: map[string]string containing the raw ConfigMap data with potential injection config
//
// Returns:
//   - SidecarInjectConfig: The parsed configuration containing main container, sidecars, and volumes
//   - error: An error if JSON unmarshaling fails, nil otherwise or when config key is missing/empty
func parseInjectConfig(ctx context.Context, configKey string, configRaw map[string]string) (SidecarInjectConfig, error) {
	log := logf.FromContext(ctx)
	sidecarConfig := SidecarInjectConfig{}

	configValue, exists := configRaw[configKey]
	if !exists || configValue == "" {
		log.Info("config key not found or empty, using default configuration")
		return sidecarConfig, nil
	}

	err := json.Unmarshal([]byte(configRaw[configKey]), &sidecarConfig)
	if err != nil {
		log.Error(err, "failed to unmarshal sidecar config for the %v", configKey)
		return sidecarConfig, err
	}
	return sidecarConfig, nil
}

// setCSIMountContainer injects CSI mount configurations into the SandboxTemplate's pod spec.
// It configures the main container (first container in the spec) with CSI sidecar settings,
// appends additional CSI sidecar containers, and mounts shared volumes.
// Volumes are only added if they don't already exist in the template.
func setCSIMountContainer(ctx context.Context, podSpec *corev1.PodSpec, config SidecarInjectConfig) {
	log := logf.FromContext(ctx)

	// set main container, the first container is the main container
	if len(podSpec.Containers) == 0 {
		log.Info("no container found in sidecar template")
		return
	}

	mainContainer := &podSpec.Containers[0]
	setMainContainerWhenInjectCSISidecar(mainContainer, config)

	// set csi sidecars
	for _, csiSidecar := range config.Sidecars {
		podSpec.Containers = append(podSpec.Containers, csiSidecar)
	}

	// set share volume
	if len(config.Volumes) > 0 {
		if podSpec.Volumes == nil {
			podSpec.Volumes = make([]corev1.Volume, 0, len(config.Volumes))
		}
		for _, vol := range config.Volumes {
			if findVolumeByName(podSpec.Volumes, vol.Name) {
				continue
			}
			podSpec.Volumes = append(podSpec.Volumes, vol)
		}
	}
}

// setMainContainerWhenInjectCSISidecar configures the main container with environment variables and volume mounts from the CSI sidecar configuration.
// It appends environment variables and volume mounts to the main container, skipping any that already exist (matched by name) to avoid duplicates.
func setMainContainerWhenInjectCSISidecar(mainContainer *corev1.Container, config SidecarInjectConfig) {
	// append some envs in main container when processing csi mount
	if mainContainer.Env == nil {
		mainContainer.Env = make([]corev1.EnvVar, 0, 1)
	}
	for _, env := range config.MainContainer.Env {
		if findEnvByName(mainContainer.Env, env.Name) {
			continue
		}
		mainContainer.Env = append(mainContainer.Env, env)
	}

	// append some volumeMounts config in main container
	if config.MainContainer.VolumeMounts != nil {
		if mainContainer.VolumeMounts == nil {
			mainContainer.VolumeMounts = make([]corev1.VolumeMount, 0, len(config.MainContainer.VolumeMounts))
		}
		for _, volMount := range config.MainContainer.VolumeMounts {
			if findVolumeMountByName(mainContainer.VolumeMounts, volMount.Name) {
				continue
			}
			mainContainer.VolumeMounts = append(mainContainer.VolumeMounts, volMount)
		}
	}
}

// setAgentRuntimeContainer injects agent runtime configurations into the SandboxTemplate's pod spec.
// It appends agent runtime containers as init containers and configures the main container (first container) with runtime settings.
// The init containers run before the main containers to prepare the runtime environment.
func setAgentRuntimeContainer(ctx context.Context, podSpec *corev1.PodSpec, config SidecarInjectConfig) {
	log := logf.FromContext(ctx)

	// append init agent runtime container
	if podSpec.InitContainers == nil {
		podSpec.InitContainers = make([]corev1.Container, 0, 1)
	}
	podSpec.InitContainers = append(podSpec.InitContainers, config.Sidecars...)

	if len(podSpec.Containers) == 0 {
		log.Info("no container found in sidecar template for agent runtime")
		return
	}
	mainContainer := &podSpec.Containers[0]
	setMainContainerConfigWhenInjectRuntimeSidecar(ctx, mainContainer, config)

	podSpec.Volumes = append(podSpec.Volumes, config.Volumes...)
}

func setMainContainerConfigWhenInjectRuntimeSidecar(ctx context.Context, mainContainer *corev1.Container, config SidecarInjectConfig) {
	log := logf.FromContext(ctx)

	// Check if main container already has a postStart hook
	if mainContainer.Lifecycle != nil && mainContainer.Lifecycle.PostStart != nil {
		if config.MainContainer.Lifecycle != nil && config.MainContainer.Lifecycle.PostStart != nil {
			log.Error(nil, "conflicting postStart hooks detected, main container already has a postStart hook defined",
				"existingHook", mainContainer.Lifecycle.PostStart,
				"injectedHook", config.MainContainer.Lifecycle.PostStart)
		}
	} else {
		// set main container lifecycle
		if mainContainer.Lifecycle == nil {
			mainContainer.Lifecycle = &corev1.Lifecycle{}
		}
		if mainContainer.Lifecycle.PostStart == nil {
			mainContainer.Lifecycle.PostStart = &corev1.LifecycleHandler{}
		}
		// Main container doesn't have postStart, apply config if available
		if config.MainContainer.Lifecycle != nil && config.MainContainer.Lifecycle.PostStart != nil {
			mainContainer.Lifecycle.PostStart = config.MainContainer.Lifecycle.PostStart
		}
	}

	// set main container env
	if mainContainer.Env == nil {
		mainContainer.Env = make([]corev1.EnvVar, 0, len(config.MainContainer.Env))
	}
	mainContainer.Env = append(mainContainer.Env, config.MainContainer.Env...)

	// set main container volumeMounts
	if mainContainer.VolumeMounts == nil {
		mainContainer.VolumeMounts = make([]corev1.VolumeMount, 0, len(config.MainContainer.VolumeMounts))
	}
	mainContainer.VolumeMounts = append(mainContainer.VolumeMounts, config.MainContainer.VolumeMounts...)
}

func findVolumeMountByName(volumeMounts []corev1.VolumeMount, name string) bool {
	for _, volumeMount := range volumeMounts {
		if volumeMount.Name == name {
			return true
		}
	}
	return false
}

func findVolumeByName(volumes []corev1.Volume, name string) bool {
	for _, volume := range volumes {
		if volume.Name == name {
			return true
		}
	}
	return false
}

func findEnvByName(envs []corev1.EnvVar, name string) bool {
	for _, env := range envs {
		if env.Name == name {
			return true
		}
	}
	return false
}
