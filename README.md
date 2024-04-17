# embedded database

go test . --coverprofile=c.out

go tool cover -html=c.out 

go test -bench=.