"""
Utility only used to perform integration tests against docker images.
"""
from docker import Client

DOCKER_BASE_URL = 'unix://var/run/docker.sock'


class DockerRun(object):
    def __init__(self, image_name, environment, ports):
        self.cli = Client(base_url=DOCKER_BASE_URL)
        self.image_name = image_name
        self.container_id = None
        self.environment = environment
        self.ports = ports


    def run(self):
        # Pull if not local
        images = self.cli.images(self.image_name)
        if len(images) == 0:
            self.cli.pull(self.image_name)
        # Run
        port_bindings = {}
        for port in self.ports:
            port_bindings[port] = ('0.0.0.0', port)
        container = self.cli.create_container(self.image_name,
                                              environment=self.environment,
                                              ports=self.ports,
                                              host_config=self.cli.create_host_config(port_bindings=port_bindings))
        self.container_id = container.get('Id')
        result = self.cli.start(container=self.container_id)


    def destroy(self):
        if self.container_id:
            self.cli.stop(container=self.container_id)
            self.cli.remove_container(container=self.container_id)
            self.container_id = None