"""Docker stuff

Instructions:
Panic ig?
"""

import geni.portal as portal
import geni.rspec.pg as rspec

request = portal.context.makeRequestRSpec()

lan = request.LAN()
for i in [1, 2, 3, 4, 5]:
    node = request.RawPC(f'node-{i}')
    node.disk_image = "urn:publicid:IDN+emulab.net+image+emulab-ops:docker-alpine3-std"
    interface = node.addInterface(f'if{i}')
    interface.component_id = f'eth{i}'
    interface.addAddress(rspec.IPv4Address(f'192.168.1.{i}', "255.255.255.0"))
    lan.addInterface(interface)

portal.context.printRequestRSpec()