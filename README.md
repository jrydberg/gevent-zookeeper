# gevent-zookeeper #

A client framework for working with Apache Zookeeper [1].

The framework draws a lot of inspriation from Netflix Curator.
If you're a Java developer, check it out.

The main class is gevent_zookeeper.ZookeeperFramework:

    >>> framework = gevent_zookeeper.ZookeeperFramework(
            'localhost:2181', 10)

The first thring you have to do is to connect to the clusteR:

    >>> framework.connect()
    >>>

Then connected, new nodes can be created:

    >>> framework.create().with_data('x').for_path('/node')

Read the docstrings for ZookeeperFramework for more information.

This code is based work done by David LaBissoniere [2].

 [1] http://zookeeper.apache.org/
 [2] https://github.com/labisso/zkproto
