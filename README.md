<div align="center">
  <h1>Programación Reactiva con Project Reactor</h1>
  <p>Documentación basada en ReactiveX y la especificación de Reactive Streams</p>
</div>

## Introducción
La programación reactiva combina las mejores ideas del patrón **Observer**, el patrón **Iterator** y la **programación funcional**.

<img width="505" height="223" alt="image" src="https://github.com/user-attachments/assets/3ddcd4b8-d977-4872-9770-c152bb8da2bc" />

Project Reactor es una librería reactiva de cuarta generación diseñada para construir aplicaciones no bloqueantes (non-blocking) en la JVM, basada fuertemente en la especificación de *Reactive Streams*.

## Componentes de la API
La API se basa fundamentalmente en cuatro componentes:
1. **Publisher** (Publicador): Proveedor de una secuencia potencialmente ilimitada de elementos.
2. **Subscriber** (Suscriptor): Quien consume los elementos emitidos.
3. **Subscription** (Suscripción): Representa la relación y ciclo de vida entre un Publisher y un Subscriber.
4. **Processor** (Procesador): Representa un estado de procesamiento que actúa tanto como Publisher como Subscriber.

## 📦 Tipos de Secuencias (Core)
Reactor ofrece dos APIs principales y componibles para manejar flujos de datos continuos:

*   **`Flux` [0|1|N]**: Secuencia que maneja de 0 a N elementos.
    ```java
    Flux<String> just = Flux.just("1", "2", "3");
    ```
*   **`Mono` [0|1]**: Secuencia especializada que emite 0 o 1 único elemento.

## ✨ Características Principales
*   **Contrapresión (Backpressure)**: Manejo eficiente de la demanda o latencia en entornos asíncronos (non-blocking).
*   **Flujos asíncronos**: Trabajan de forma nativa con flujos de datos continuos.
*   **Inmutabilidad**: Las secuencias son inmutables por diseño.
*   **Cancelables**: Las operaciones y flujos pueden ser cancelados en cualquier momento.
*   **Flexibilidad de creación**: Pueden ser creados desde otras fuentes como *streams*, listas, intervalos, rangos, etc.
*   **Finitos o infinitos**: Soportan secuencias que tienen un fin definido o que son continuas (infinitas).
*   **Concurrencia y Errores**: Hacen que la concurrencia sea fácil de manejar. Proveen mecanismos asíncronos para el manejo de errores (como re-intentos automáticos si algo falla).
*   **Multi-plataforma**: Los conceptos (ReactiveX) son aplicables en múltiples ecosistemas como Java, JavaScript, Python, C++, entre otros.

## 🛠️ Operadores Clave (Comportamiento visual)
Los operadores reducen notablemente las tareas manuales de transformación. A continuación, el comportamiento documentado de los operadores visualizados en los diagramas de canicas (Marble Diagrams):

### 1. `map`
Transforma los elementos emitidos por el flujo de manera individual.
*   *Comportamiento visual:* Si el flujo emite los valores `(1, 2, 3)` y aplicamos la función `map(x => 10 * x)`, el resultado es un nuevo flujo idéntico en tiempos pero con los valores transformados a `(10, 20, 30)`.

### 2. `filter`
Evalúa cada valor y descarta aquellos que no cumplan una condición.
*   *Comportamiento visual:* Dado un flujo con `(2, 30, 22, 5, 60, 1)`, al aplicar `filter(x => x > 10)`, los números menores o iguales a 10 desaparecen, dejando un flujo limpio con `(30, 22, 60)`.

### 3. `delay` (Retardo temporal)
Desplaza la emisión de los valores en el tiempo sin alterar su orden.
*   *Comportamiento visual:* Los elementos entran al operador y se retienen (ej. `delay(20)`). Luego son emitidos respetando la separación original entre ellos, pero desplazados temporalmente hacia el futuro.

### 4. `merge`
Combina múltiples flujos independientes (Publishers) en un solo flujo, intercalando los elementos en el orden de tiempo exacto en que ocurren.
*   *Comportamiento visual:* Si la Línea A emite `(20, 40, 60, 80, 100)` y la Línea B emite `(1, 1)` en diferentes momentos, el flujo combinado fusiona ambos entrelazando los eventos resultando en `(20, 40, 60, 1, 80, 100, 1)`.
README.md
Mostrando README.md.

<h1>Funciones Flux</h1>
Métodos de CreaciónFlux.just(...): Crea un flujo reactivo que emite los elementos proporcionados directamente en sus parámetros.  Flux.range(...): Crea un flujo que emite una secuencia incremental de números enteros dentro de un rango especificado.  Flux.create(...): Permite construir un flujo asíncrono desde cero mediante un emisor (FluxSink), muy útil para integrar lógica tradicional (como Timer) al mundo reactivo.  Flux.interval(...): Genera un flujo que emite números secuenciales (de tipo Long) espaciados por una duración de tiempo específica.  Flux.fromIterable(...): Convierte una estructura de datos tradicional e iterable (como una List de Java) en un flujo reactivo que emite cada elemento de la colección.  Flux.error(...): Crea un flujo que termina inmediatamente emitiendo un error (una excepción) hacia el suscriptor.  Operadores de Transformación y Filtrado.map(...): Transforma los elementos del flujo aplicando una operación de forma síncrona, modificando el valor o el tipo del elemento emitido.  .flatMap(...): Transforma los elementos de manera asíncrona devolviendo un nuevo Mono o Flux por cada elemento, y luego "aplana" todos esos resultados en un solo flujo continuo.  .filter(...): Evalúa cada elemento contra una condición lógica y solo deja continuar en el flujo a los elementos que la cumplen.  Operadores de Utilidad y Efectos Secundarios.doOnNext(...): Permite observar cada elemento que viaja por el flujo y ejecutar un bloque de código (como imprimir en consola o lanzar una validación) sin modificar el elemento original.  .doOnTerminate(...): Registra una acción o callback que se ejecutará justo antes de que el flujo termine, independientemente de si terminó con éxito o con un error.  .delayElements(...): Retrasa artificialmente la emisión de cada elemento en el flujo por un periodo de tiempo determinado.  .limitRate(...): Herramienta clave para manejar la contrapresión (backpressure), limitando la cantidad de elementos que se solicitan y procesan en "lotes".  .log(): Imprime automáticamente en la consola las señales reactivas del flujo (onSubscribe, request, onNext, onComplete) para facilitar la depuración.  .retry(...): Permite volver a intentar la ejecución del flujo un número determinado de veces en caso de que ocurra un error.  Operadores de Combinación.zipWith(...): Toma el flujo actual y lo combina con otro, sincronizando las emisiones y uniendo los elementos de ambos flujos en un solo resultado (por defecto una Tupla).  Operadores Finales (Terminales).collectList(): Recopila todos los elementos emitidos por el Flux y los agrupa en una única lista de Java contenida dentro de un Mono.  .subscribe(...): Es el gatillo que inicia la ejecución real de todo el flujo reactivo y define qué hacer cuando llegan los datos, los errores o cuando se completa el proceso.  .blockLast(): Bloquea el hilo de ejecución actual esperando activamente a que el flujo reactivo termine y retorne el último valor emitido.  
