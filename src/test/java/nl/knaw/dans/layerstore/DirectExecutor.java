package nl.knaw.dans.layerstore;

import java.util.concurrent.Executor;

class DirectExecutor implements Executor {

    @Override
    public void execute(Runnable runnable) {
        runnable.run();
    }
}