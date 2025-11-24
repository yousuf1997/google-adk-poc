package com.google.adk.runner;

import com.google.adk.agents.BaseAgent;
import com.google.adk.agents.RunConfig;
import com.google.adk.artifacts.InMemoryArtifactService;
import com.google.adk.events.Event;
import com.google.adk.memory.InMemoryMemoryService;
import com.google.adk.session.MongoDBSessionService;
import com.google.adk.sessions.Session;
import com.google.genai.types.Content;
import com.google.genai.types.Part;
import io.reactivex.rxjava3.core.Flowable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Scanner;

import static java.nio.charset.StandardCharsets.UTF_8;

@Component
public class MongoDBSessionRunner extends Runner {

    public MongoDBSessionRunner(BaseAgent agent, MongoDBSessionService mongoDBSessionService, @Value("${spring.application.name}") String appName) {
        super(agent, appName, new InMemoryArtifactService(), mongoDBSessionService, new InMemoryMemoryService());
    }

    public void runAgent() {
        RunConfig runConfig = RunConfig.builder().build();

        Session session = this
                .sessionService()
                .createSession(this.appName(), "user1234")
                .blockingGet();

        try (Scanner scanner = new Scanner(System.in, UTF_8)) {
            while (true) {
                System.out.print("\nYou > ");
                String userInput = scanner.nextLine();
                if ("quit".equalsIgnoreCase(userInput)) {
                    break;
                }

                Content userMsg = Content.fromParts(Part.fromText(userInput));
                Flowable<Event> events = this.runAsync(session.userId(), session.id(), userMsg, runConfig);

                System.out.print("\nAgent > ");
                events.blockingForEach(event -> {
                    if (event.finalResponse()) {
                        System.out.println(event.stringifyContent());
                    }
                });

            }
        }
    }

}
