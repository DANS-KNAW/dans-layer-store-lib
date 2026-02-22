Live tests
==========

Some of the unit tests are live tests, meaning they require a connection to a remote server. Normally these tests are skipped.
To enable them, copy the file `example-live-test.properties` to `live-test.properties` and fill in the values. Then run the
tests (they are annotated with `@EnabledIf("nl.knaw.dans.layerstore.TestConditions#dmftarLiveTestConfigured")`).

Tests involving dmftar
----------------------
Some of the live tests involve dmftar. When running these tests, make sure that the following commands are on the local PATH:

* `dmftar`
* `dmftar-volume-changer`
* `tar`

The first two can best be installed in a Python virtual environment. You can then set the bin directory of the virtual environment
on the PATH. `tar` should be available on any Linux system.

Note that this also holds for running these tests from IntelliJ (and possibly other IDEs). In IntelliJ, configure the PATH via Modify Run Configuration for the
test. You may have to do this for each test if you want to run individual test methods.