"""Rabia UDP Testing

Instructions:
Panic ig?
"""

import geni.portal as portal
import geni.rspec.pg as rspec

request = portal.context.makeRequestRSpec()

lan = request.LAN()
for i in [1, 2, 3, 4, 5, 6]: #, 4, 5
    node = request.RawPC("node-" + str(i))
    node.hardware_type = "d430" #d710
    node.disk_image = "urn:publicid:IDN+emulab.net+image+HyflowTM:rs-rabia-testing"
    lan.addInterface(interface)

portal.context.printRequestRSpec()
