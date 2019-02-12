package devtools.lib.rxui;

import java.util.function.Consumer;

import javafx.application.Application;
import javafx.stage.Stage;

public class FxApp extends Application {

    public static Consumer<Stage> appRunner;

    @Override
    public void start(Stage primaryStage) throws Exception {
        appRunner.accept(primaryStage);
    }

    public static void main(String[] args) {
        launch();
    }
}
