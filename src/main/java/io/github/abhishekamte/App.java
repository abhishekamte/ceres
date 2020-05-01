package io.github.abhishekamte;


public class App {

    private LeaderService leaderService;

    void setup() {
        leaderService = new LeaderService();
    }

    void myAwesomeAppLogic() {
        try {
            Thread.sleep(60 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    void shutdown() {
        leaderService.close();
    }


    public static void main(String[] args){
        App app = new App();
        app.setup();
        app.myAwesomeAppLogic();
        app.shutdown();
    }
}