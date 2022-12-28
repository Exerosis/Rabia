"""Rabia UDP Testing

Instructions:
Panic ig?
"""

import geni.portal as portal
import geni.rspec.pg as rspec

request = portal.context.makeRequestRSpec()

lan = request.LAN()
for i in [1, 2, 3]: #, 4, 5
    node = request.RawPC("node-" + str(i))
    node.hardware_type = "d710" #d430
    node.disk_image = "urn:publicid:IDN+emulab.net+image+HyflowTM:Rabia-Kotlin-UDP.base"
#     node.addService(RSpec.Execute(
#         shell="sh", command="sudo /local/scripts/startup.sh"
#     ))
    interface = node.addInterface("if" + str(i))
    interface.component_id = "eth" + str(i)
    interface.addAddress(rspec.IPv4Address("192.168.1." + str(i), "255.255.255.0"))
    lan.addInterface(interface)

portal.context.printRequestRSpec()