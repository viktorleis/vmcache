from osv.modules import api

default = api.run("/vmcache")
scalability_test = api.run("--env=VIRTGB=128 --env=PHYSGB=64 --env=DATASIZE=32 --env=RUNFOR=60 vmcache")
