#!/bin/sh
echo "Changing ownership for all config folders to $(RUN_AS_USER)"
chown -R $(RUN_AS_USER) conf/$(PRODUCT_NAME)
chown -R $(RUN_AS_USER) logs/$(PRODUCT_NAME)
chown -R $(RUN_AS_USER) var/$(PRODUCT_NAME)/run
chown -R $(RUN_AS_USER) var/jvmdump
