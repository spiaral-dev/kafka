package Producer;

import java.util.List;

public class KafkaHandlerConfig {

    private String topicName;

    private List<String> bootstrapServers;

    String getTopicName() {
        return topicName;
    }

    void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    List<String> getBootstrapServers() {
        return bootstrapServers;
    }

    void setBootstrapServers(List<String> bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }
}
