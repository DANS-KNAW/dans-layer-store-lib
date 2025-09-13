Live tests
==========

Some of the unit tests are live tests, meaning they require a connection to a remote server. Normally these tests are skipped. 
To enable them, copy the file `example-live-test.properties` to `live-test.properties` and fill in the values. Then run the
tests (they are annotated with `@EnabledIf("nl.knaw.dans.layerstore.TestConditions#dmftarLiveTestConfigured")`).
