
#  Example pseudo code for the Actifio primary-primary setup:
for each-mdb-node-that-has-been-tagged-to-one-of-the-Actifio-primaries:
    if last-oplog-chunk-succeeded and no-elections-since-the-last-oplog-chunk:
        feed-the-next-chunk-to-the-local-actifio
    else:
        skip-this-chunk-on-this-node
