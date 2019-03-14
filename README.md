# reactive jms es cqrs app
In progress: Reactive EventSourced CQRS JMS app

<!--
![](./logo.png)
-->

<img src="./logo.png" alt="drawing" width="50%" />

```bash
# run app
./mvnw spring-boot:run
# subscribe server side events
http --stream :8080/event-stream
# send messages
http :8080 msg=hello
http :8080 msg=world
```
