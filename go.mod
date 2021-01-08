module uta.edu/aces/jade-go

go 1.15

require (
	github.com/Azure/go-autorest/autorest v0.11.1 // indirect
	github.com/StephanDollberg/go-json-rest-middleware-jwt v0.0.0-20160723210644-be2a0500d9b3
	github.com/ant0ine/go-json-rest v3.3.2+incompatible
	github.com/ant0ine/go-json-rest-middleware-statsd v0.0.0-20160102230551-d75044bee493
	github.com/coreos/go-semver v0.3.0
	github.com/dgrijalva/jwt-go v3.2.0+incompatible // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/jadengore/go-json-rest-middleware-force-ssl v0.0.0-20151221030829-998b54ac67ff
	github.com/jinzhu/gorm v1.9.16
	github.com/peterbourgon/g2s v0.0.0-20170223122336-d4e7ad98afea // indirect
	github.com/shykes/spdy-go v0.0.0-20130118032238-0ee95b1b8e48
	github.com/yvasiyarov/go-metrics v0.0.0-20150112132944-c25f46c4b940
	github.com/yvasiyarov/gorelic v0.0.7
	github.com/yvasiyarov/newrelic_platform_go v0.0.0-20160601141957-9c099fbc30e9 // indirect
	golang.org/x/net v0.0.0-20200822124328-c89045814202
	gonum.org/v1/gonum v0.8.2
	gopkg.in/tylerb/graceful.v1 v1.2.15
	gopkg.in/yaml.v2 v2.3.0 // indirect
	k8s.io/api v0.19.0
	k8s.io/apimachinery v0.19.0
	k8s.io/client-go v0.19.0
	uta.edu/aces/jadesdk v0.0.0
)

replace uta.edu/aces/jadesdk => ../jadesdk
