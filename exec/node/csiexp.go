/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package node

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/chaosblade-io/chaosblade-spec-go/spec"
	"github.com/chaosblade-io/chaosblade-spec-go/util"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/chaosblade-io/chaosblade-operator/channel"
	"github.com/chaosblade-io/chaosblade-operator/exec/model"
	"github.com/chaosblade-io/chaosblade-operator/pkg/apis/chaosblade/v1alpha1"
)

const (
	CsiSocketPathFlag = "csi-socket-path"
)

func NewCsiExpModelCommandSpec(client *channel.Client) spec.ExpModelCommandSpec {
	return &CsiExpModelCommandSpec{
		spec.BaseExpModelCommandSpec{
			ExpActions: []spec.ExpActionCommandSpec{
				NewCsiMountFaultActionSpec(client),
				NewCsiUnmountFaultActionSpec(client),
			},
			ExpFlags: []spec.ExpFlagSpec{},
		},
	}
}

type CsiExpModelCommandSpec struct {
	spec.BaseExpModelCommandSpec
}

func (*CsiExpModelCommandSpec) Name() string {
	return "csi"
}

func (*CsiExpModelCommandSpec) ShortDesc() string {
	return "CSI fault experiment"
}

func (*CsiExpModelCommandSpec) LongDesc() string {
	return "CSI fault experiment, simulate CSI driver socket failures on the node"
}

func (*CsiExpModelCommandSpec) Example() string {
	return `# Auto-discover CSI socket via K8s API
blade create k8s node-csi mount_fault --names cn-hangzhou.192.168.0.205 --kubeconfig ~/.kube/config
# Or specify explicitly
blade create k8s node-csi mount_fault --csi-socket-path /var/lib/kubelet/plugins/disk.csi.example.com/csi.sock --names cn-hangzhou.192.168.0.205 --kubeconfig ~/.kube/config`
}

// CsiMountFaultActionSpec

func NewCsiMountFaultActionSpec(client *channel.Client) spec.ExpActionCommandSpec {
	return &CsiMountFaultActionSpec{
		spec.BaseExpActionCommandSpec{
			ActionMatchers: []spec.ExpFlagSpec{
				&spec.ExpFlag{
					Name: CsiSocketPathFlag,
					Desc: "The full path of the CSI driver socket. If not specified, auto-discovered via CSINode API",
				},
			},
			ActionFlags:      []spec.ExpFlagSpec{},
			ActionExecutor:   &CsiFaultExecutor{client: client, faultType: "mount"},
			ActionCategories: []string{model.CategorySystemContainer},
			ActionExample: `# Simulate CSI mount failure with auto-discovered CSI socket
blade create k8s node-csi mount_fault --names cn-hangzhou.192.168.0.205 --kubeconfig ~/.kube/config
# Or specify the CSI socket path explicitly
blade create k8s node-csi mount_fault --csi-socket-path /var/lib/kubelet/plugins/disk.csi.example.com/csi.sock --names cn-hangzhou.192.168.0.205 --kubeconfig ~/.kube/config`,
		},
	}
}

type CsiMountFaultActionSpec struct {
	spec.BaseExpActionCommandSpec
}

func (*CsiMountFaultActionSpec) Name() string {
	return "mount_fault"
}

func (*CsiMountFaultActionSpec) Aliases() []string {
	return []string{}
}

func (*CsiMountFaultActionSpec) ShortDesc() string {
	return "Simulate CSI mount failure"
}

func (*CsiMountFaultActionSpec) LongDesc() string {
	return "Simulate CSI mount failure by making the CSI socket unreachable, new pods will be stuck in ContainerCreating"
}

// CsiUnmountFaultActionSpec

func NewCsiUnmountFaultActionSpec(client *channel.Client) spec.ExpActionCommandSpec {
	return &CsiUnmountFaultActionSpec{
		spec.BaseExpActionCommandSpec{
			ActionMatchers: []spec.ExpFlagSpec{
				&spec.ExpFlag{
					Name: CsiSocketPathFlag,
					Desc: "The full path of the CSI driver socket. If not specified, auto-discovered via CSINode API",
				},
			},
			ActionFlags:      []spec.ExpFlagSpec{},
			ActionExecutor:   &CsiFaultExecutor{client: client, faultType: "unmount"},
			ActionCategories: []string{model.CategorySystemContainer},
			ActionExample: `# Simulate CSI unmount failure with auto-discovered CSI socket
blade create k8s node-csi unmount_fault --names cn-hangzhou.192.168.0.205 --kubeconfig ~/.kube/config
# Or specify the CSI socket path explicitly
blade create k8s node-csi unmount_fault --csi-socket-path /var/lib/kubelet/plugins/disk.csi.example.com/csi.sock --names cn-hangzhou.192.168.0.205 --kubeconfig ~/.kube/config`,
		},
	}
}

type CsiUnmountFaultActionSpec struct {
	spec.BaseExpActionCommandSpec
}

func (*CsiUnmountFaultActionSpec) Name() string {
	return "unmount_fault"
}

func (*CsiUnmountFaultActionSpec) Aliases() []string {
	return []string{}
}

func (*CsiUnmountFaultActionSpec) ShortDesc() string {
	return "Simulate CSI unmount failure"
}

func (*CsiUnmountFaultActionSpec) LongDesc() string {
	return "Simulate CSI unmount failure by making the CSI socket unreachable, terminating pods will be stuck"
}

// CsiFaultExecutor

type CsiFaultExecutor struct {
	client    *channel.Client
	faultType string // "mount" or "unmount"
}

func (e *CsiFaultExecutor) Name() string {
	return "csi_fault"
}

func (e *CsiFaultExecutor) SetChannel(channel spec.Channel) {
}

func (e *CsiFaultExecutor) Exec(uid string, ctx context.Context, expModel *spec.ExpModel) *spec.Response {
	if _, ok := spec.IsDestroy(ctx); ok {
		return e.destroy(uid, ctx, expModel)
	}
	return e.create(uid, ctx, expModel)
}

func (e *CsiFaultExecutor) create(uid string, ctx context.Context, expModel *spec.ExpModel) *spec.Response {
	logrusField := logrus.WithField("experiment", model.GetExperimentIdFromContext(ctx))
	containerObjectMetaList, err := model.GetContainerObjectMetaListFromContext(ctx)
	if err != nil {
		util.Errorf(uid, util.GetRunFuncName(), err.Error())
		return spec.ResponseFailWithResult(spec.ContainerInContextNotFound,
			v1alpha1.CreateFailExperimentStatus(err.Error(), []v1alpha1.ResourceStatus{}))
	}

	csiSocketPath := expModel.ActionFlags[CsiSocketPathFlag]

	statuses := make([]v1alpha1.ResourceStatus, 0)
	success := true
	updateLock := &sync.Mutex{}

	execFunc := func(i int) {
		meta := containerObjectMetaList[i]
		status := v1alpha1.ResourceStatus{
			Kind:       "node",
			Identifier: meta.GetIdentifier(),
			Id:         uid,
		}

		daemonsetPodName, err := model.GetChaosBladeDaemonsetPodName(meta.NodeName, e.client)
		if err != nil {
			logrusField.Errorf("get chaosblade daemonset pod on node %s failed: %v", meta.NodeName, err)
			status = status.CreateFailResourceStatus(err.Error(), spec.K8sExecFailed.Code)
			updateLock.Lock()
			statuses = append(statuses, status)
			success = false
			updateLock.Unlock()
			return
		}
		if daemonsetPodName == "" {
			errMsg := fmt.Sprintf("chaosblade daemonset pod not found on node %s", meta.NodeName)
			logrusField.Error(errMsg)
			status = status.CreateFailResourceStatus(errMsg, spec.K8sExecFailed.Code)
			updateLock.Lock()
			statuses = append(statuses, status)
			success = false
			updateLock.Unlock()
			return
		}

		resolvedPath := csiSocketPath
		if resolvedPath == "" {
			discovered, discoverErr := discoverCsiSocketPath(e.client, meta.NodeName)
			if discoverErr != nil {
				errMsg := fmt.Sprintf("auto-discover CSI socket on node %s failed: %v", meta.NodeName, discoverErr)
				logrusField.Error(errMsg)
				status = status.CreateFailResourceStatus(errMsg, spec.K8sExecFailed.Code)
				updateLock.Lock()
				statuses = append(statuses, status)
				success = false
				updateLock.Unlock()
				return
			}
			logrusField.Infof("auto-discovered CSI socket: %s on node %s", discovered, meta.NodeName)
			resolvedPath = discovered
		}

		script := generateCsiCreateScript(resolvedPath)
		resp := execScriptInDaemonsetPod(e.client, daemonsetPodName, script)
		if resp.Success {
			status = status.CreateSuccessResourceStatus()
		} else {
			status = status.CreateFailResourceStatus(resp.Err, spec.K8sExecFailed.Code)
			success = false
		}
		updateLock.Lock()
		statuses = append(statuses, status)
		updateLock.Unlock()
	}

	model.ParallelizeExec(len(containerObjectMetaList), execFunc)
	logrusField.Infof("csi %s fault create result, success: %t, statuses: %+v", e.faultType, success, statuses)

	if success {
		return spec.ReturnResultIgnoreCode(v1alpha1.CreateSuccessExperimentStatus(statuses))
	}
	return spec.ReturnResultIgnoreCode(v1alpha1.CreateFailExperimentStatus("see resStatuses for details", statuses))
}

func (e *CsiFaultExecutor) destroy(uid string, ctx context.Context, expModel *spec.ExpModel) *spec.Response {
	logrusField := logrus.WithField("experiment", model.GetExperimentIdFromContext(ctx))
	containerObjectMetaList, err := model.GetContainerObjectMetaListFromContext(ctx)
	if err != nil {
		util.Errorf(uid, util.GetRunFuncName(), err.Error())
		return spec.ResponseFailWithResult(spec.ContainerInContextNotFound,
			v1alpha1.CreateFailExperimentStatus(err.Error(), []v1alpha1.ResourceStatus{}))
	}

	csiSocketPath := expModel.ActionFlags[CsiSocketPathFlag]

	statuses := make([]v1alpha1.ResourceStatus, 0)
	success := true
	updateLock := &sync.Mutex{}

	execFunc := func(i int) {
		meta := containerObjectMetaList[i]
		status := v1alpha1.ResourceStatus{
			Kind:       "node",
			Identifier: meta.GetIdentifier(),
			Id:         meta.Id,
			State:      v1alpha1.DestroyedState,
		}

		daemonsetPodName, err := model.GetChaosBladeDaemonsetPodName(meta.NodeName, e.client)
		if err != nil {
			logrusField.Errorf("get chaosblade daemonset pod on node %s failed: %v", meta.NodeName, err)
			status = status.CreateFailResourceStatus(err.Error(), spec.K8sExecFailed.Code)
			updateLock.Lock()
			statuses = append(statuses, status)
			success = false
			updateLock.Unlock()
			return
		}
		if daemonsetPodName == "" {
			errMsg := fmt.Sprintf("chaosblade daemonset pod not found on node %s", meta.NodeName)
			logrusField.Error(errMsg)
			status = status.CreateFailResourceStatus(errMsg, spec.K8sExecFailed.Code)
			updateLock.Lock()
			statuses = append(statuses, status)
			success = false
			updateLock.Unlock()
			return
		}

		resolvedPath := csiSocketPath
		if resolvedPath == "" {
			discovered, discoverErr := discoverCsiSocketPath(e.client, meta.NodeName)
			if discoverErr != nil {
				errMsg := fmt.Sprintf("auto-discover CSI socket on node %s failed: %v", meta.NodeName, discoverErr)
				logrusField.Error(errMsg)
				status = status.CreateFailResourceStatus(errMsg, spec.K8sExecFailed.Code)
				updateLock.Lock()
				statuses = append(statuses, status)
				success = false
				updateLock.Unlock()
				return
			}
			logrusField.Infof("auto-discovered CSI socket: %s on node %s", discovered, meta.NodeName)
			resolvedPath = discovered
		}

		script := generateCsiDestroyScript(resolvedPath)
		resp := execScriptInDaemonsetPod(e.client, daemonsetPodName, script)
		if resp.Success {
			status.Success = true
		} else {
			status = status.CreateFailResourceStatus(resp.Err, spec.K8sExecFailed.Code)
			success = false
		}
		updateLock.Lock()
		statuses = append(statuses, status)
		updateLock.Unlock()
	}

	model.ParallelizeExec(len(containerObjectMetaList), execFunc)
	logrusField.Infof("csi fault destroy result, success: %t, statuses: %+v", success, statuses)

	if success {
		return spec.ReturnResultIgnoreCode(v1alpha1.CreateDestroyedExperimentStatus(statuses))
	}
	return spec.ReturnResultIgnoreCode(v1alpha1.CreateFailExperimentStatus("see resStatuses for details", statuses))
}

func discoverCsiSocketPath(client *channel.Client, nodeName string) (string, error) {
	ctx := context.TODO()

	// Step 1: Get CSINode to find registered drivers
	csiNode, err := client.StorageV1().CSINodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get CSINode for node %s: %v", nodeName, err)
	}
	drivers := csiNode.Spec.Drivers
	if len(drivers) == 0 {
		return "", fmt.Errorf("no CSI drivers registered on node %s", nodeName)
	}
	if len(drivers) > 1 {
		names := make([]string, 0, len(drivers))
		for _, d := range drivers {
			names = append(names, d.Name)
		}
		return "", fmt.Errorf("multiple CSI drivers found on node %s: %s. Please specify --csi-socket-path",
			nodeName, strings.Join(names, ", "))
	}

	// Step 2: List all pods on this node, filter DaemonSet pods, extract --kubelet-registration-path
	podList, err := client.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + nodeName,
	})
	if err != nil {
		return "", fmt.Errorf("failed to list pods on node %s: %v", nodeName, err)
	}

	var registrationPaths []string
	for _, pod := range podList.Items {
		// Filter: only DaemonSet-owned pods
		isDaemonSet := false
		for _, ownerRef := range pod.OwnerReferences {
			if ownerRef.Kind == "DaemonSet" {
				isDaemonSet = true
				break
			}
		}
		if !isDaemonSet {
			continue
		}

		// Search all container args for --kubelet-registration-path
		for _, container := range pod.Spec.Containers {
			for i, arg := range container.Args {
				if strings.HasPrefix(arg, "--kubelet-registration-path=") {
					path := strings.TrimPrefix(arg, "--kubelet-registration-path=")
					registrationPaths = append(registrationPaths, path)
				} else if arg == "--kubelet-registration-path" && i+1 < len(container.Args) {
					registrationPaths = append(registrationPaths, container.Args[i+1])
				}
			}
		}
	}

	// Step 3: Decide based on results
	if len(registrationPaths) == 0 {
		return "", fmt.Errorf("no --kubelet-registration-path found in DaemonSet pods on node %s", nodeName)
	}
	if len(registrationPaths) > 1 {
		return "", fmt.Errorf("multiple CSI socket paths found on node %s: %s. Please specify --csi-socket-path",
			nodeName, strings.Join(registrationPaths, ", "))
	}

	return registrationPaths[0], nil
}

func generateCsiCreateScript(csiSocketPath string) string {
	backupPath := csiSocketPath + ".chaosblade.bak"
	script := fmt.Sprintf(`SOCK_PATH='%s'
BACKUP_PATH='%s'
if [ ! -S "$SOCK_PATH" ]; then
  if [ -S "$BACKUP_PATH" ]; then
    echo '{"code":409,"success":false,"error":"CSI fault already injected, backup exists: '"$BACKUP_PATH"'"}'
    exit 0
  fi
  echo '{"code":404,"success":false,"error":"CSI socket not found: '"$SOCK_PATH"'"}'
  exit 0
fi
mv "$SOCK_PATH" "$BACKUP_PATH" 2>/dev/null
if [ $? -ne 0 ]; then
  echo '{"code":500,"success":false,"error":"failed to rename CSI socket, permission denied"}'
  exit 0
fi
echo '{"code":200,"success":true}'
`, csiSocketPath, backupPath)

	return script
}

func generateCsiDestroyScript(csiSocketPath string) string {
	backupPath := csiSocketPath + ".chaosblade.bak"
	script := fmt.Sprintf(`SOCK_PATH='%s'
BACKUP_PATH='%s'
if [ ! -S "$BACKUP_PATH" ]; then
  echo '{"code":200,"success":true}'
  exit 0
fi
mv "$BACKUP_PATH" "$SOCK_PATH" 2>/dev/null
if [ $? -ne 0 ]; then
  echo '{"code":500,"success":false,"error":"failed to restore CSI socket"}'
  exit 0
fi
echo '{"code":200,"success":true}'
`, csiSocketPath, backupPath)

	return script
}
