package com.mohamed.google_adk_poc;

import com.google.adk.agents.LlmAgent;
import com.google.adk.agents.RunConfig;
import com.google.adk.artifacts.InMemoryArtifactService;
import com.google.adk.events.Event;
import com.google.adk.memory.InMemoryMemoryService;
import com.google.adk.runner.InMemoryRunner;
import com.google.adk.runner.Runner;
import com.google.adk.sessions.Session;
import com.google.genai.types.Content;
import com.google.genai.types.Part;
import com.mohamed.google_adk_poc.session.MongoDBSessionService;
import io.reactivex.rxjava3.core.Flowable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.Scanner;

import static java.nio.charset.StandardCharsets.UTF_8;

@SpringBootApplication
public class GoogleAdkPocApplication implements CommandLineRunner {

	@Autowired
	private LlmAgent historyAgent;

	@Autowired
	private MongoTemplate mongoTemplate;

	public static void main(String[] args) {
		SpringApplication.run(GoogleAdkPocApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		RunConfig runConfig = RunConfig.builder().build();

		MongoDBSessionService mongoDBSessionService = new MongoDBSessionService(mongoTemplate, "adk_poc_session_and_user_state", "adk_poc_app_state");

		Runner runner = new Runner(historyAgent, "adk-poc", new InMemoryArtifactService(), mongoDBSessionService, new InMemoryMemoryService());

		Session session = runner
				.sessionService()
				.createSession(runner.appName(), "user1234")
				.blockingGet();

		try (Scanner scanner = new Scanner(System.in, UTF_8)) {
			while (true) {
				System.out.print("\nYou > ");
				String userInput = scanner.nextLine();
				if ("quit".equalsIgnoreCase(userInput)) {
					break;
				}

				Content userMsg = Content.fromParts(Part.fromText(userInput));
				Flowable<Event> events = runner.runAsync(session.userId(), session.id(), userMsg, runConfig);

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
