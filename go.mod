module uta.edu/aces/jade-go

go 1.15

require (
	github.com/NYTimes/gziphandler v1.1.1
	github.com/ant0ine/go-json-rest v3.3.2+incompatible
	github.com/stretchr/testify v1.8.2
	golang.org/x/exp v0.0.0-20230321023759-10a507213a29
	gonum.org/v1/gonum v0.12.0
	k8s.io/api v0.27.1
	k8s.io/apimachinery v0.27.1
	k8s.io/client-go v0.25.1
	uta.edu/aces/jadesdk v0.0.0
)

replace uta.edu/aces/jadesdk => ../jadesdk
