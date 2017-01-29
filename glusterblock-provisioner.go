package main

import (
	"errors"
	"flag"
	"os"
	exec "os/exec"
	"time"
	"fmt"
	dstrings "strings"
	"strconv"

	"github.com/golang/glog"
	"github.com/kubernetes-incubator/nfs-provisioner/controller"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api/v1"
	volutil "k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/client-go/pkg/types"
	"k8s.io/client-go/pkg/util/uuid"
	"k8s.io/client-go/pkg/util/wait"
	"k8s.io/client-go/rest"
)

const (
	resyncPeriod              = 15 * time.Second
	provisionerName           = "gluster.org/glusterblock"
	exponentialBackOffOnError = false
	failedRetryThreshold      = 5
	provisionerMode		  	  = "script"
	secretKeyName 			  = "key" 
)

type glusterBlockProvisioner struct {
	// Kubernetes Client. Use to retrieve Gluster admin secret
	client kubernetes.Interface

	// Identity of this glusterBlockProvisioner, generated. Used to identify "this"
	// provisioner's PVs.
	identity types.UID

	provConfig provisionerConfig
}

type provisionerConfig struct {
	// Required:  this is the Rest Service Url ( ex: Heketi) for Gluster Block
	url string `json:"url"`

	// Optional: Rest user who is capable of creating gluster block volumes.
	user string `json:"user,omitempty"`

	// Optional: Rest user key for above RestUser.
	userKey string `json:"key,omitempty"`

	// Optional: Operation mode  (rest, script)
	opMode string `json:"opmode,omitempty"`

	// Optional: Script path if we are operating in script mode.
	scriptPath string `json:"scriptpath,omitempty"`

	// Optional:  secret name, namespace.
	secretNamespace string 	`json:"secretnamespace, omitempty"`
	secretName      string  `json:"secret, omitempty"`
	secretValue     string  `json:"secretvalue, omitempty"`
	
	// Optinal:  ClusterID from which the provisioner create the block volume
	clusterId       string  `json:"cluster, omitempty"`
	
	// Optional: high availability count
	haCount int `json:"hacount, omitempty"`



}

func NewglusterBlockProvisioner(client kubernetes.Interface) controller.Provisioner {
	return &glusterBlockProvisioner{
		client: client,
		identity: uuid.NewUUID(),
	}
}

var _ controller.Provisioner = &glusterBlockProvisioner{}

// Provision creates a storage asset and returns a PV object representing it.
func (p *glusterBlockProvisioner) Provision(options controller.VolumeOptions) (*v1.PersistentVolume, error) {
    var err error

    glog.V(4).Infof("glusterblock: Provison VolumeOptions %v", options)
	//scName := storageutil.GetClaimStorageClass(r.options.PVC)
	cfg, err := parseClassParameters(options.Parameters, p.client)
	if err != nil {
		return nil, err
	}
	p.provConfig = *cfg

	glog.V(4).Infof("glusterfs: creating volume with configuration %+v", p.provConfig)
    // TODO: 
	if options.PVC.Spec.Selector != nil {
		return nil, fmt.Errorf("claim Selector is not supported")
	}
	server, path, err := p.createVolume(options.PVName)
	if err != nil {
		return nil, err
	}
	glog.V(1).Infof("Server and path returned :%v %v", server, path)

	pv := &v1.PersistentVolume{
		ObjectMeta: v1.ObjectMeta{
			Name: options.PVName,
			Annotations: map[string]string{
				"glusterBlockProvisionerIdentity": string(p.identity),
			},
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: options.PersistentVolumeReclaimPolicy,
			AccessModes:                   options.PVC.Spec.AccessModes,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)],
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				ISCSI: &v1.ISCSIVolumeSource{
					TargetPortal: server,
					IQN:          path,
					Lun:          0,
					FSType:       "ext3",
					ReadOnly:     false,
				},
			},
		},
	}

	return pv, nil
}

// createVolume creates a gluster block volume i.e. the storage asset.

func (p *glusterBlockProvisioner) createVolume(PVName string) (string, string, error) {
	var dtarget, diqn string
	if p.provConfig.opMode == "script" {
		cmd := exec.Command("sh", p.provConfig.scriptPath)
		err := cmd.Run()
		if err != nil {
			glog.Errorf("%v", err)
		}
		dtarget = os.Getenv("SERVER")
		diqn = os.Getenv("IQN")
	}
	return dtarget, diqn, nil
}

// Delete removes the storage asset that was created by Provision represented
// by the given PV.
func (p *glusterBlockProvisioner) Delete(volume *v1.PersistentVolume) error {
	ann, ok := volume.Annotations["glusterBlockProvisionerIdentity"]
	if !ok {
		return errors.New("identity annotation not found on PV")
	}
	if ann != string(p.identity) {
		return &controller.IgnoredError{"identity annotation on PV does not match ours"}
	}

	return nil
}

// delete removes the directory backing the given PV that was created by
// createVolume.
func (p *glusterBlockProvisioner) delete(volume *v1.PersistentVolume) error {
	//TODO: Write delete function
	return nil
}


func parseClassParameters(params map[string]string, kubeclient kubernetes.Interface) (*provisionerConfig, error) {
	var cfg provisionerConfig
	var err error

	authEnabled := true
	haCount := 3
	parseVolumeType := ""
	for k, v := range params {
		switch dstrings.ToLower(k) {
		case "resturl":
			cfg.url = v
		case "restuser":
			cfg.user = v
		case "restuserkey":
			cfg.userKey = v
		case "secretname":
			cfg.secretName = v
		case "secretnamespace":
			cfg.secretNamespace = v
		case "clusterid":
			if len(v) != 0 {
				cfg.clusterId = v
			}
		case "restauthenabled":
			authEnabled = dstrings.ToLower(v) == "true"
		
		case "hacount":
			haCount, err = strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("glusterblock: failed to parse hacount %v ", k)
			}

		default:
			return nil, fmt.Errorf("glusterblock: invalid option %q for volume plugin %s", k, "glusterblock")
		}
	}

	if len(cfg.url) == 0 {
		return nil, fmt.Errorf("StorageClass for provisioner %s must contain 'resturl' parameter", "glusterblock")
	}

	if haCount == 0 {
		cfg.haCount = 3
	} 
	
	if !authEnabled {
		cfg.user = ""
		cfg.secretName = ""
		cfg.secretNamespace = ""
		cfg.userKey = ""
		cfg.secretValue = ""
	}

	if len(cfg.secretName) != 0 || len(cfg.secretNamespace) != 0 {
		// secretName + Namespace has precedence over userKey
		if len(cfg.secretName) != 0 && len(cfg.secretNamespace) != 0 {
			cfg.secretValue, err = parseSecret(cfg.secretNamespace, cfg.secretName, kubeclient)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("StorageClass for provisioner %q must have secretNamespace and secretName either both set or both empty", "glusterblock")
		}
	} else {
		cfg.secretValue = cfg.userKey
	}

	return &cfg, nil
}


// parseSecret finds a given Secret instance and reads user password from it.
func parseSecret(namespace, secretName string, kubeClient kubernetes.Interface) (string, error) {
	//secretMap, err := volutil.GetSecretForPV(namespace, secretName, "glusterblock", kubeClient)
	secretMap, err := GetSecretForPV(namespace, secretName, "glusterblock", kubeClient)
	if err != nil {
		glog.Errorf("failed to get secret %s/%s: %v", namespace, secretName, err)
		return "", fmt.Errorf("failed to get secret %s/%s: %v", namespace, secretName, err)
	}
	if len(secretMap) == 0 {
		return "", fmt.Errorf("empty secret map")
	}
	secret := ""
	for k, v := range secretMap {
		if k == secretKeyName {
			return v, nil
		}
		secret = v
	}
	// If not found, the last secret in the map wins as done before
	return secret, nil
}


// GetSecretForPV locates secret by name and namespace, verifies the secret type, and returns secret map
func GetSecretForPV(secretNamespace, secretName, volumePluginName string, kubeClient clientset.Interface) (map[string]string, error) {
        secret := make(map[string]string)
        if kubeClient == nil {
                return secret, fmt.Errorf("Cannot get kube client")
        }
        secrets, err := kubeClient.Core().Secrets(secretNamespace).Get(secretName, metav1.GetOptions{})
        if err != nil {
                return secret, err
        }
        if secrets.Type != v1.SecretType(volumePluginName) {
                return secret, fmt.Errorf("Cannot get secret of type %s", volumePluginName)
        }
        for name, data := range secrets.Data {
                secret[name] = string(data)
        }
        return secret, nil
}


func main() {
	flag.Parse()
	flag.Set("logtostderr", "true")

	// Create an InClusterConfig and use it to create a client for the controller
	// to use to communicate with Kubernetes
	config, err := rest.InClusterConfig()
	if err != nil {
		glog.Fatalf("Failed to create config: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		glog.Fatalf("Failed to create client: %v", err)
	}

	// The controller needs to know what the server version is because out-of-tree
	// provisioners aren't officially supported until 1.5
	serverVersion, err := clientset.Discovery().ServerVersion()
	if err != nil {
		glog.Fatalf("Error getting server version: %v", err)
	}

	// Create the provisioner: it implements the Provisioner interface expected by
	// the controller
	glusterBlockProvisioner := NewglusterBlockProvisioner(clientset)

	// Start the provision controller which will dynamically provision glusterblock
	// PVs
	pc := controller.NewProvisionController(clientset, resyncPeriod, provisionerName, glusterBlockProvisioner, serverVersion.GitVersion, exponentialBackOffOnError, failedRetryThreshold)
	pc.Run(wait.NeverStop)
}
