# How to run the test

```bash
cd jade-go/scheduler
go test -run ''
go test -bench .
```

to run specific test, like `initiating`:
```bash
go test -run 'Initiating'
```