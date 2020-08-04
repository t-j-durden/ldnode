from ldnode import *

# todo: rename ldnode
# todo: use case:lk,az,tbl
# todo: publisher - save to self option
# todo: lookup function for mapping
# todo: cleanup the publisher code (publisher to have classes in config for parser etc when this is done)
# todo: git source
# todo: add unit tests
# todo: cleanup data file test_data
# todo: watch file , raise event on publisher and VoID file
# todo: security (ssl + auth on: publisher, sparql and query )
# todo: type templates and PyNode template


n = LdNode(config='config.ttl', uri='https://test.people', uri_stub='https://person/')
n.start()
#n.stop()



