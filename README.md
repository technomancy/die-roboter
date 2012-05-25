# Die Roboter

> Wir laden unsere Batterie.  
> Jetzt sind wir voller Energie.
>
> Wir sind die Roboter.
>
> Wir funktionieren automatik.  
> Jetzt wollen wir tanzen mechanik.
>
> Wir sind die Roboter.
>
> Wir sind auf Alles programmiert.  
> Und was du willst wird ausgeführt.
> 
> - Die Roboter by Kraftwerk

<img src="http://technomancy.us/i/die_roboter.jpg" align="right" />

The Robots get your work done in an straightforward way.

## Usage

```clj
(ns die.roboter.example
  (:require [die.roboter :as roboter]))

;; starting up
(roboter/work) ; on the worker nodes (this will block)

;; ordering around
(roboter/send-off `(println "Boing, Boom Tschak.")) ; returns immediately

(let [f (roboter/send-back `(slurp "/etc/hosts"))]
  (println @f)) ; when you need a return value back
```

Jobs will not ack to the server until they've completed successfully,
so workers that throw exceptions or disappear entirely will have their
jobs automatically retried.

Exceptions occurring in `send-back` calls will be propagated back to the
caller and thrown upon deref. When using `send-off` by default failed
jobs will simply log using `clojure.tools.logging/warn`, but you can
rebind `*exception-handler*` to respond in your own way, including
acking the message back to the server:

```clj
(defn handle-tachyon [e msg]
  (if (re-find #"tachyon" (.getMessage e)) ; tachyon failures don't get retried
    (com.mefesto.wabbitmq/ack (-> msg :envelope :delivery-tag))
    (println "Oh, we got trouble!" (.getMessage e))))

(binding [roboter/*exception-handler* handle-tachyon]
  (roboter/work))
```

By default each job has five minutes to complete before it is
considered hung and are killed, returning its work to the queue. The
timeout can be overridden by rebinding `die.roboter/*timeout*` or
passing in a `:timeout` config key to the `work` function. You can
also avoid timing out by calling the `die.roboter/report-progress`
function, which resets the timer back to the start.

AMQP is used as the transport. [RabbitMQ](http://rabbitmq.com) is a
popular choice. Most functions take an optional `config` argument that
can be used to specify the AMQP connection settings, but you can also
use the `with-robots` macro to bind it dynamically.

```clj
(roboter/send-off '(println "Greetings, programs") {:host "10.1.12.99"})

(roboter/with-robots {:username "flynn" :password "reindeerflotilla"}
  (roboter/send-off `(println "Started working on" ~hostname))
  (roboter/work))
```

## License

Image and lyrics quoted above copyright © 1979 Kraftwerk.

Code copyright © 2011 Phil Hagelberg.

Distributed under the Eclipse Public License, the same as Clojure.
