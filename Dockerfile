FROM google/golang
ADD . /gopath/src/github.com/marconi/scylla
RUN go get -t github.com/marconi/scylla
WORKDIR /gopath/src/github.com/marconi/scylla
