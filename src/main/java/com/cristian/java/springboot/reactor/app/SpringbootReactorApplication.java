package com.cristian.java.springboot.reactor.app;

import com.cristian.java.springboot.reactor.app.models.Comments;
import com.cristian.java.springboot.reactor.app.models.User;
import com.cristian.java.springboot.reactor.app.models.UserComments;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class SpringbootReactorApplication implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(SpringbootReactorApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(SpringbootReactorApplication.class, args);
    }

    /**
     * Método de la interfaz CommandLineRunner que se ejecuta al iniciar la aplicación.
     * Aquí se llama al método de prueba que se desee ejecutar.
     */
    @Override
    public void run(String... args) throws Exception {
        backPressure();
    }

    /**
     * Demuestra la creación de un Flux básico y la suscripción al mismo.
     * Utiliza doOnNext para realizar efectos secundarios (imprimir, transformar, validar)
     * antes de que el suscriptor final procese los datos.
     */
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
    }

    /**
     * Explora el concepto de Contrapresión (Backpressure).
     * Muestra cómo limitar la cantidad de elementos que el suscriptor solicita al publicador usando limitRate.
     */
    private void backPressure() {
        Flux.range(1, 10)
                .log()
                .limitRate(5)
                .subscribe(value -> log.info(value.toString()));
    }

    /**
     * Muestra cómo crear un Flux asíncrono desde cero utilizando Flux.create y un Timer.
     * Emite eventos periódicamente y maneja la finalización exitosa o con errores del flujo.
     */
    private void intervalFromCreate() {
        Flux.create(emmiter -> {
                    Timer timmer = new Timer();
                    timmer.schedule(new TimerTask() {
                        private Integer counter = 0;

                        @Override
                        public void run() {
                            emmiter.next(++counter);
                            if (counter == 5) {
                                timmer.cancel();
                                emmiter.error(new InterruptedException("Error, se ha detenido el flujo en 5"));
                            }
                        }
                    }, 1000, 1000);
                })
                .doOnTerminate(() -> log.info("Hemos terminado"))
                .retry(2)
                .subscribe(
                        next -> log.info(next.toString()),
                        e -> log.error(e.getMessage()),
                        () -> log.info("Hemos completado correctamente")
                );
    }

    /**
     * Utiliza Flux.interval para emitir elementos indefinidamente con un retraso,
     * usando CountDownLatch para evitar que el hilo principal se cierre antes de procesar el flujo.
     */
    private void intervalInfinite() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Flux.interval(Duration.ofSeconds(1))
                .doOnTerminate(countDownLatch::countDown)
                .flatMap(value -> {
                    if (value == 5) {
                        return Flux.error(new InterruptedException("Solo hasta 5"));
                    }
                    return Flux.just(value);
                })
                .map(i -> "Hola ".concat(i.toString()))
                .retry(2)
                .subscribe(log::info, error -> log.error(error.getMessage()));

        countDownLatch.await();
    }

    /**
     * Aplica un retardo a la emisión de cada elemento en un flujo utilizando delayElements.
     * Retiene la ejecución del hilo principal con CountDownLatch.
     */
    private void delayElements() throws InterruptedException {
        CountDownLatch counter = new CountDownLatch(1);

        Flux<Integer> range = Flux.range(1, 12)
                .delayElements(Duration.ofSeconds(2))
                .doOnNext(value -> log.info(value.toString()))
                .doOnTerminate(counter::countDown);

        range.subscribe();
        counter.await();
    }

    /**
     * Combina un flujo de rango con un flujo de intervalos temporales utilizando zipWith.
     * Esto simula la emisión paulatina (con delay) de elementos de un flujo finito.
     */
    private void interval() {
        Flux<Integer> range = Flux.range(1, 12);
        Flux<Long> delay = Flux.interval(Duration.ofSeconds(1));

        range.zipWith(delay, (first, second) -> first)
                .doOnNext(value -> log.info(value.toString()))
                .blockLast();
    }

    /**
     * Muestra cómo combinar dos flujos distintos (range y numbers) en uno solo (Tuplas) mediante zipWith.
     */
    private void zipRange() {
        Flux<Integer> range = Flux.range(0, 5);
        Flux<Integer> numbers = Flux.just(1, 2, 3, 4, 5);

        numbers.map(integer -> integer * 2)
                .zipWith(range, (firts, second) -> String.format("Primer Flux: %d, Segundo Flux %d", firts, second))
                .subscribe(log::info);
    }

    /**
     * Método auxiliar para generar un objeto de tipo User.
     * @return una instancia estática de User.
     */
    private User createUser() {
        return new User("John", "Doe");
    }

    /**
     * Combina un Mono de User y un Mono de Comments usando zipWith.
     * Mapea las respuestas extrayendo los elementos de la Tupla (T1 y T2) resultante.
     */
    private void userCommentsZipWith2() {
        Mono<User> userMono = Mono.fromCallable(this::createUser);
        Mono<Comments> commentsMono = Mono.fromCallable(() -> {
            Comments comments = new Comments();
            comments.addComment("Hola pepe, que tal!");
            comments.addComment("Mañana voy a la playa!");
            comments.addComment("Etoy en la seccion de spring con reactor!");
            return comments;
        });

        Mono<UserComments> userCommentsMono = userMono.zipWith(commentsMono)
                .map(tuple -> {
                    User user = tuple.getT1();
                    Comments comments = tuple.getT2();
                    return new UserComments(user, comments);
                });
        userCommentsMono.subscribe(userComments -> {
            System.out.println(userComments);
        });
    }

    /**
     * Forma alternativa de combinar dos Monos (User y Comments) con zipWith,
     * proporcionando una función combinadora directamente.
     */
    private void userCommentsZipWith() {
        Mono<User> userMono = Mono.fromCallable(this::createUser);
        Mono<Comments> commentsMono = Mono.fromCallable(() -> {
            Comments comments = new Comments();
            comments.addComment("Hola pepe, que tal!");
            comments.addComment("Mañana voy a la playa!");
            comments.addComment("Etoy en la seccion de spring con reactor!");
            return comments;
        });

        Mono<UserComments> userCommentsMono = userMono.zipWith(commentsMono, (user, comments) -> new UserComments(user, comments));
        userCommentsMono.subscribe(userComments -> {
            System.out.println(userComments);
        });
    }

    /**
     * Combina dos Monos distintos utilizando flatMap de manera anidada.
     * Al resolverse ambos, se crea el objeto contenedor final (UserComments).
     */
    private void userCommentsFlatmap() {

        Mono<User> userMono = Mono.fromCallable(this::createUser);
        Mono<Comments> commentsMono = Mono.fromCallable(() -> {
            Comments comments = new Comments();
            comments.addComment("Hola pepe, que tal!");
            comments.addComment("Mañana voy a la playa!");
            comments.addComment("Etoy en la seccion de spring con reactor!");
            return comments;
        });

        Mono<UserComments> userCommentsMono = userMono
                .flatMap(user -> commentsMono
                        .flatMap(comments -> Mono.fromCallable(() -> new UserComments(user, comments))));

        userCommentsMono.subscribe(userComments -> log.info(userComments.toString()));
    }

    /**
     * Convierte un Flux (que emite varios elementos) en un Mono de una Lista (Mono<List<T>>).
     * Útil cuando se necesita recopilar todo el flujo en una sola estructura de datos antes de continuar.
     */
    private void collectList() {
        List<User> userList = Arrays.asList(
                new User("Cristian", "Cristaldo"),
                new User("Diego", "Fulano"),
                new User("Maria", "Fulana"),
                new User("Pedro", "Mengano"),
                new User("Bruce", "Doe"),
                new User("Juan", "Doe"),
                new User("Bruce", "Sultano"));

        Mono<List<String>> names = Flux.fromIterable(userList)
                .flatMap(user -> Mono.just(user.getName().concat(" ").concat(user.getLastname())))
                .collectList();

        names.subscribe(list -> list.forEach(System.out::println));
    }

    /**
     * Utiliza flatMap para transformar un objeto User a String, e incluye lógica
     * para filtrar emitiendo un Mono vacío (Mono.empty()) si no se cumple una condición.
     */
    private void flatMapToString() {
        List<User> userList = new ArrayList<>();
        userList.add(new User("Cristian", "Cristaldo"));
        userList.add(new User("Diego", "Fulano"));
        userList.add(new User("Maria", "Fulana"));
        userList.add(new User("Pedro", "Mengano"));
        userList.add(new User("Bruce", "Doe"));
        userList.add(new User("Juan", "Doe"));
        userList.add(new User("Bruce", "Sultano"));

        Flux<User> names = Flux.fromIterable(userList);

        Flux<String> users = names
                .flatMap(user -> Mono.just(user.getName().concat(" ").concat(user.getLastname())))
                .flatMap(user -> {
                    if (user.toLowerCase().contains("bruce")) {
                        return Mono.just(user);
                    }
                    return Mono.empty();
                })
                .map(String::toUpperCase);

        users.subscribe((log::info));
    }

    /**
     * Toma una lista de strings y utiliza operadores reactivos (map, flatMap) para convertirlos
     * en objetos User, filtrar ciertos usuarios, modificar sus atributos y pasar los elementos al suscriptor final.
     */
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

        Flux<User> users = names
                .map(name -> {
                    return new User(name.split(" ")[0], name.split(" ")[1]);
                })
                .flatMap(user -> {
                    if (user.getName().equalsIgnoreCase("bruce")) {
                        return Mono.just(user);
                    }
                    return Mono.empty();
                })
                .map(user -> {
                    String lastnameUpperCase = user.getLastname().toUpperCase();
                    user.setLastname(lastnameUpperCase);
                    user.setCreatedAt(LocalDateTime.now());
                    return user;
                });

        users.subscribe((user -> log.info(user.toString())));
    }

    /**
     * Crea un Flux directamente desde una interfaz Iterable (como una List).
     * Muestra el encadenamiento de operadores para transformar y validar los datos secuencialmente.
     */
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

    /**
     * Muestra la ejecución en cascada combinando el operador filter con map.
     * Descarta en etapas tempranas aquellos elementos que no cumplen con los requerimientos (ej. longitud del nombre).
     */
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

    /**
     * Transforma un Flux original emitiendo el tamaño de cada cadena recibida, convirtiéndolo efectivamente
     * en un Flux de enteros (Flux<Integer>). Valida y lanza excepción en caso de recibir cadenas vacías.
     */
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

    /**
     * Operador básico map usado para alterar un String convirtiéndolo a mayúsculas.
     * Se usa doOnNext para inspeccionar o modificar estados fuera del flujo de transformación (validación).
     */
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