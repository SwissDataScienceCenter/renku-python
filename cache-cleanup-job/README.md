# Core Service cache cleanup image
Small image to be used for the cache cleanup CronJob for the core service.

Loops through endpoint slices and call the cleanup endpoint on each core-svc instance.

Push as `renku/renku-core-cleanup:<version>` to use
