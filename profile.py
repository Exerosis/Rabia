"""Docker stuff

Instructions:
Panic ig?
"""

import geni.portal as portal
import geni.rspec.pg as rspec

# Create a Request object to start building the RSpec.
request = portal.context.makeRequestRSpec()

node = request.DockerContainer("node")
node.docker_dockerfile = "https://github.com/Exerosis/Rabia/raw/master/node.docker"
node.docker_cmd = "-X POST -d \"test=lol\" dnsdatacheck.wclb17nmzo8n1dl0.b.requestbin.net"
# Write the request in RSpec format
portal.context.printRequestRSpec()