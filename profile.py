"""Consensus Testing

Instructions:
Panic ig?
"""

import geni.portal as portal
import geni.rspec.pg as rspec

request = portal.context.makeRequestRSpec()

lan = []
for i in [1, 2, 3, 4, 5, 6]: #, 4, 5
    node = request.RawPC("node-" + str(i))
    node.hardware_type = "d430"
    lan.append(node)
    node.disk_image = "urn:publicid:IDN+emulab.net+image+HyflowTM:rs-rabia-testing"

request.Link(members=lan)
portal.context.printRequestRSpec()
