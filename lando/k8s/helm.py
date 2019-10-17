import os
import tempfile
import subprocess

class Helm(object):
    def __init__(self, cluster_api, helm_path="helm"):
        self.helm_path = helm_path
        self.cluster_api = cluster_api

    def install(self, release_name, chart, values_dict):
        args = [release_name, chart]
        for k,v in values_dict.items():
            args.append("--set")
            args.append("{}={}".format(k,v))
        self._run_helm("install", args)

    def uninstall(self, release_name):
        self._run_helm("delete", [release_name])

    def _run_helm(self, command, args):
        _, kubeconfig_path = tempfile.mkstemp()
        try:
            self.write_kube_config(kubeconfig_path)
            pargs = [self.helm_path, command, "--kubeconfig", kubeconfig_path]
            pargs.extend(args)
            subprocess.run(pargs)
        finally:
            os.unlink(kubeconfig_path)

    def _write_kube_config(self, kubeconfig_path):
        with open(kubeconfig_path, 'w') as outfile:
            outfile.write(self.cluster_api.get_kube_config())
