"""An example of constructing a profile with a single Xen VM.

Instructions:
Wait for the profile instance to start, and then log in to the VM via the
ssh port specified below.  (Note that in this case, you will need to access
the VM through a high port on the physical host, since we have not requested
a public IP address for the VM itself.)
"""

# Import the Portal object.
import geni.portal as portal
import geni.rspec.pg as rspec

# Create a Request object to start building the RSpec.
request = portal.context.makeRequestRSpec()

node = request.DockerContainer("node")
node.docker_dockerfile = "https://github.com/docker-library/httpd/raw/38842a5d4cdd44ff4888e8540c0da99009790d01/2.4/Dockerfile"

# Add a XenVM (named "node") to the request
node = request.XenVM("node")

# Write the request in RSpec format
portal.context.printRequestRSpec()