from kubernetes import client, config, watch
import logging

RESTART_POLICY = "Never"


class AccessModes(object):
    READ_WRITE_MANY = "ReadWriteMany"
    READ_WRITE_ONCE = "ReadWriteOnce"
    READ_ONLY_MANY = "ReadOnlyMany"


class JobConditionType:
    COMPLETE = "Complete"
    FAILED = "Failed"


class ClusterApi(object):
    def __init__(self, host, token, namespace, verify_ssl=True):
        configuration = client.Configuration()
        configuration.host = host
        configuration.api_key = {"authorization": "Bearer " + token}
        configuration.verify_ssl = verify_ssl
        self.api_client = client.ApiClient(configuration)
        self.core = client.CoreV1Api(self.api_client)
        self.batch = client.BatchV1Api(self.api_client)
        self.namespace = namespace

    def create_persistent_volume_claim(self, name, storage_size_in_g, storage_class_name,
                                       access_modes=[AccessModes.READ_WRITE_MANY]):
        pvc = client.V1PersistentVolumeClaim()
        pvc.metadata = client.V1ObjectMeta(name=name)
        storage_size = "{}Gi".format(storage_size_in_g)
        resources = client.V1ResourceRequirements(requests={"storage": storage_size})
        pvc.spec = client.V1PersistentVolumeClaimSpec(access_modes=access_modes,
                                                      resources=resources,
                                                      storage_class_name=storage_class_name)
        return self.core.create_namespaced_persistent_volume_claim(self.namespace, pvc)

    def delete_persistent_volume_claim(self, name):
        self.core.delete_namespaced_persistent_volume_claim(name, self.namespace, client.V1DeleteOptions())

    def create_secret(self, name, string_value_dict):
        body = client.V1Secret(string_data=string_value_dict, metadata={'name': name})
        return self.core.create_namespaced_secret(namespace=self.namespace, body=body)

    def delete_secret(self, name):
        self.core.delete_namespaced_secret(name, self.namespace, body=client.V1DeleteOptions())

    def create_job(self, name, batch_job_spec):
        body = client.V1Job(
            metadata=client.V1ObjectMeta(name=name),
            spec=batch_job_spec.create())
        return self.batch.create_namespaced_job(self.namespace, body)

    def wait_for_job_events(self, callback, label_selector=None):
        w = watch.Watch()
        for event in w.stream(self.batch.list_namespaced_job, self.namespace, label_selector=label_selector):
            job = event['object']
            callback(job)

    def delete_job(self, name, propagation_policy='Background'):
        body = client.V1DeleteOptions(propagation_policy=propagation_policy)
        self.batch.delete_namespaced_job(name, self.namespace, body=body)

    def create_config_map(self, name, data):
        body = client.V1ConfigMap(
            metadata=client.V1ObjectMeta(name=name),
            data=data
        )
        return self.core.create_namespaced_config_map(self.namespace, body)

    def delete_config_map(self, name):
        self.core.delete_namespaced_config_map(name, self.namespace, body=client.V1DeleteOptions())

    def read_pod_logs(self, name):
        return self.core.read_namespaced_pod_log(name, self.namespace)


class Container(object):
    def __init__(self, name, image_name, command, args=[], working_dir=None, env_dict={},
                 requested_cpu=None, requested_memory=None, volumes=[]):
        self.name = name
        self.image_name = image_name
        self.command = command
        self.args = args
        self.working_dir = working_dir
        self.env_dict = env_dict
        self.requested_cpu = requested_cpu
        self.requested_memory = requested_memory
        self.volumes = volumes

    def create_env(self):
        environment_variables = []
        for key, value in self.env_dict.items():
            if isinstance(value, EnvVarSource):
                environment_variables.append(client.V1EnvVar(name=key, value_from=value.create_env_var_source()))
            else:
                environment_variables.append(client.V1EnvVar(name=key, value=value))
        return environment_variables

    def create_volume_mounts(self):
        return [volume.create_volume_mount() for volume in self.volumes]

    def create_volumes(self):
        return [volume.create_volume() for volume in self.volumes]

    def create_resource_requirements(self):
        return client.V1ResourceRequirements(
            requests={
                "memory": self.requested_memory,
                "cpu": self.requested_cpu
            })

    def create(self):
        return client.V1Container(
            name=self.name,
            image=self.image_name,
            working_dir=self.working_dir,
            command=self.command,
            args=self.args,
            resources=self.create_resource_requirements(),
            env=self.create_env(),
            volume_mounts=self.create_volume_mounts()
        )


class EnvVarSource(object):
    def create_env_var_source(self):
        raise NotImplementedError("Subclasses of EnvVarSource should implement create_env_var_source.")


class SecretEnvVar(EnvVarSource):
    def __init__(self, name, key):
        self.name = name
        self.key = key

    def create_env_var_source(self):
        return client.V1EnvVarSource(
            secret_key_ref=client.V1SecretKeySelector(
                key=self.key,
                name=self.name
            )
        )


class FieldRefEnvVar(EnvVarSource):
    def __init__(self, field_path):
        self.field_path = field_path

    def create_env_var_source(self):
        return client.V1EnvVarSource(
            field_ref=client.V1ObjectFieldSelector(field_path=self.field_path)
        )


class VolumeBase(object):
    """
    Base class that represents a volume that will be mounted.
    """
    def __init__(self, name, mount_path):
        self.name = name
        self.mount_path = mount_path

    def create_volume_mount(self):
        return client.V1VolumeMount(
            name=self.name,
            mount_path=self.mount_path)

    def create_volume(self):
        raise NotImplementedError("Subclasses of VolumeBase should implement create_volume.")


class SecretVolume(VolumeBase):
    def __init__(self, name, mount_path, secret_name):
        super(SecretVolume, self).__init__(name, mount_path)
        self.secret_name = secret_name

    def create_volume(self):
        return client.V1Volume(
                name=self.name,
                secret=self.create_secret())

    def create_secret(self):
        return client.V1SecretVolumeSource(secret_name=self.secret_name)


class PersistentClaimVolume(VolumeBase):
    def __init__(self, name, mount_path, volume_claim_name, read_only=False):
        super(PersistentClaimVolume, self).__init__(name, mount_path)
        self.volume_claim_name = volume_claim_name
        self.read_only = read_only

    def create_volume(self):
        return client.V1Volume(
            name=self.name,
            persistent_volume_claim=self.create_volume_source())

    def create_volume_source(self):
        return client.V1PersistentVolumeClaimVolumeSource(
            claim_name=self.volume_claim_name,
            read_only=self.read_only)


class ConfigMapVolume(VolumeBase):
    def __init__(self, name, mount_path, config_map_name, source_key, source_path):
        super(ConfigMapVolume, self).__init__(name, mount_path)
        self.config_map_name = config_map_name
        self.source_key = source_key
        self.source_path = source_path

    def create_volume(self):
        return client.V1Volume(
            name=self.name,
            config_map=self.create_config_map())

    def create_config_map(self):
        items = [client.V1KeyToPath(key=self.source_key, path=self.source_path)]
        return client.V1ConfigMapVolumeSource(name=self.config_map_name,
                                              items=items)


class BatchJobSpec(object):
    def __init__(self, name, container, labels={}):
        self.name = name
        self.pod_restart_policy = RESTART_POLICY
        self.container = container
        self.labels = labels

    def create(self):
        job_spec_name = "{}spec".format(self.name)
        return client.V1JobSpec(
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(name=job_spec_name, labels=self.labels),
                spec=self.create_pod_spec()
            )
        )

    def create_pod_spec(self):
        return client.V1PodSpec(
            containers=self.create_containers(),
            volumes=self.create_volumes(),
            restart_policy=RESTART_POLICY,
        )

    def create_containers(self):
        container = self.container.create()
        return [container]

    def create_volumes(self):
        return self.container.create_volumes()