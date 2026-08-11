package com.cristian.java.springboot.reactor.app;

import ch.qos.logback.core.encoder.JsonEscapeUtil;
import com.cristian.java.springboot.reactor.app.models.User;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.core.publisher.Flux;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
public class SpringbootReactorApplication implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(SpringbootReactorApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(SpringbootReactorApplication.class, args);
    }

    private void subscribeAndOnNext() {
        Flux<String> names = Flux.just("Andres", "Diego", "Maria", "Pedro", "Juan", "Bruce")
                .doOnNext(System.out::println)
                .doOnNext(name -> System.out.println(name.toUpperCase()))
                .doOnNext(name -> {
                            if (name.isBlank()) {
                                throw new RuntimeException("Los nombres no pueden ser vacios");
                            }
                            System.out.println(name.toLowerCase());
                        }
                );

        names.subscribe(log::info,
                error -> log.error(error.getMessage()),
                () -> log.info("El flujo del Observable ha finalizado correctamente"));

//        names.subscribe(new Subscriber<String>() {
//
//            @Override
//            public void onSubscribe(Subscription s) {
//                s.request(6);
//            }
//
//            @Override
//            public void onNext(String s) {
//                log.info(s);
//            }
//
//            @Override
//            public void onError(Throwable t) {
//                log.error(t.getMessage());
//            }
//
//            @Override
//            public void onComplete() {
//                log.info("El flujo del Observable ha finalizado correctamente");
//            }
//        });
    }

    @Override
    public void run(String... args) throws Exception {
        flatMap();
    }

    private void flatMap() {
        List<String> userList = new ArrayList<>();
        userList.add("Cristian Cristaldo");
        userList.add("Diego Fulano");
        userList.add("Maria Fulana");
        userList.add("Pedro Mengano");
        userList.add("Bruce Doe");
        userList.add("Juan Doe");
        userList.add("Bruce Sultano");

        Flux<String> names = Flux.fromIterable(userList);

        Flux<User> users = names.map(name -> {
                    return new User(name.split(" ")[0], name.split(" ")[1]);
                })
                .doOnNext(System.out::println)
                .filter(user -> user.getName().equalsIgnoreCase("bruce"))
                .doOnNext(user -> {
                    if (user.getName().isBlank()) {
                        throw new RuntimeException("Los nombres no pueden ser vacios o tener una cantidad de caracteres igual a 0");
                    }
                    System.out.println(user.getName().length());
                })
                .map(user -> {
                    String lastnameUpperCase = user.getLastname().toUpperCase();
                    user.setLastname(lastnameUpperCase);
                    user.setCreatedAt(LocalDateTime.now());
                    return user;
                });

        users.subscribe((user -> log.info(user.toString())),
                error -> log.error(error.getMessage()),
                () -> log.info("El flujo del Observable ha finalizado correctamente"));
    }

    private void fronIterable() {
        List<String> userList = new ArrayList<>();
        userList.add("Cristian Cristaldo");
        userList.add("Diego Fulano");
        userList.add("Maria Fulana");
        userList.add("Pedro Mengano");
        userList.add("Bruce Doe");
        userList.add("Juan Doe");
        userList.add("Bruce Sultano");

        Flux<String> names = Flux.fromIterable(userList);

        Flux<User> users = names.map(name -> {
                    return new User(name.split(" ")[0], name.split(" ")[1]);
                })
                .doOnNext(System.out::println)
                .filter(user -> user.getName().equalsIgnoreCase("bruce"))
                .doOnNext(user -> {
                    if (user.getName().isBlank()) {
                        throw new RuntimeException("Los nombres no pueden ser vacios o tener una cantidad de caracteres igual a 0");
                    }
                    System.out.println(user.getName().length());
                })
                .map(user -> {
                    String lastnameUpperCase = user.getLastname().toUpperCase();
                    user.setLastname(lastnameUpperCase);
                    user.setCreatedAt(LocalDateTime.now());
                    return user;
                });

        users.subscribe((user -> log.info(user.toString())),
                error -> log.error(error.getMessage()),
                () -> log.info("El flujo del Observable ha finalizado correctamente"));
    }

    private void mapAndFilter3User() {
        Flux<String> names = Flux.just("Cristian Cristaldo", "Diego Fulano", "Maria Fulana", "Pedro Mengano", "Bruce Doe", "Juan Doe", "Bruce Sultano");

        Flux<User> users = names.map(name -> {
                    return new User(name.split(" ")[0], name.split(" ")[1]);
                })
                .filter(user -> user.getName().length() == 5)
                .doOnNext(System.out::println)
                .filter(user -> user.getName().equalsIgnoreCase("bruce"))
                .doOnNext(user -> {
                    if (user.getName().isBlank()) {
                        throw new RuntimeException("Los nombres no pueden ser vacios o tener una cantidad de caracteres igual a 0");
                    }
                    System.out.println(user.getName().length());
                })
                .map(user -> {
                    String lastnameUpperCase = user.getLastname().toUpperCase();
                    user.setLastname(lastnameUpperCase);
                    user.setCreatedAt(LocalDateTime.now());
                    return user;
                });

        names.subscribe(System.out::println);

        users.subscribe((user -> log.info(user.toString())),
                error -> log.error(error.getMessage()),
                () -> log.info("El flujo del Observable ha finalizado correctamente"));
    }

    private void mapAndFilter2Integer() {
        Flux<Integer> names = Flux.just("Andres", "Diego", "Maria", "Pedro", "Juan", "Bruce")
                .map(name -> {
                    return name.length();
                })
                .doOnNext(l -> {
                    if (l == 0) {
                        throw new RuntimeException("Los nombres no pueden ser vacios o tener una cantidad de caracteres igual a 0");
                    }
                    System.out.println(l);
                });

        names.subscribe((value -> log.info(value.toString())),
                error -> log.error(error.getMessage()),
                () -> log.info("El flujo del Observable ha finalizado correctamente"));
    }

    private void mapAndFilter() {
        Flux<String> names = Flux.just("Andres", "Diego", "Maria", "Pedro", "Juan", "Bruce")
                .map(name -> {
                    return name.toUpperCase();
                })
                .doOnNext(name -> {
                    if (name.isBlank()) {
                        throw new RuntimeException("Los nombres no pueden ser vacios");
                    }
                    System.out.println(name);
                });

        names.subscribe(log::info,
                error -> log.error(error.getMessage()),
                () -> log.info("El flujo del Observable ha finalizado correctamente"));
    }

}
