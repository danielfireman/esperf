# Contributing to esperf

Found a problem and would like to fix it? Have that great idea and would love to see it done? Let's do it!

> Please open an issue before start working

That could save a lot of time from everyone and we are super happy to answer questions and help you alonge the way.

This project shares Go's code of conduct [values](https://golang.org/conduct#values) and [unwelcomed behavior](https://golang.org/conduct#unwelcome_behavior). Not sure what those mean or why we need those? Please give yourself a few minutes to get acquainted to those topics.

* Before start coding:
     * Fork and pull the latest version of the master branch
     * Make sure you have go 1.8+ installed and you're using it

* Requirements
    * Compliance with [these guidelines](https://code.google.com/p/go-wiki/wiki/CodeReviewComments)
    * Good unit test coverage
    * [Good commit messages](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html)

* Before sending the PR

```sh
$ cd $GOPATH/src/github.com/danielfireman/esperf
$ ./fmt.sh
$ go test ./..
```

If all tests pass, you're ready to send the PR.
