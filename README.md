# emqx_plugin_kafka

Kafka plugin for EMQX V5.4 版本，支持EMQX转Kafka配置SASL/PLAIN模式。

## Usage

### Build the EMQX broker
* 本文中内容，在RockyLinux 8.5 和 RockyLinux 9.2 中环境下，已经验证过。

* 先安装相关依赖组件，通过dnf方式安装。
```shell
dnf -y install gcc gcc-c++ cpp glibc  glibc-devel glibc-headers kernel-devel kernel-headers cmake make m4 ncurses ncurses-devel openssl openssl-devel openssl-libs zlib zlib-devel libselinux-devel xmlto perl git wget zip unzip gtk2-devel binutils-devel unixODBC libtool wxWidgets bzip2 binutils-devel  
```


* 安装Erlang/OTP   ( emqx v5.4.0 安装 25.3.2.5 )

```
下载地址
https://www.erlang.org/patches/otp-25.3.2.5 

解压 Erlang
tar xvf  otp_src_25.3.2.5.tar.gz
 
进入解压目录
cd otp_src_25.3.2.5

编译  
./configure --prefix=/usr/local/erlang --with-ssl --enable-threads --enable-smp-support --enable-kernel-poll --enable-hipe --without-javac

编译并安装
make && make install

修改环境变量
vim /etc/profile
 
将以下内容存储至profile文件中，保存并退出
export ERLPATH=/usr/local/erlang
export PATH=$ERLPATH/bin:$PATH
 
使环境变量刷新并生效
source /etc/profile
 
验证erlang是否安装成功
erl

```


* 安装 Rebar3    (  rebar3 安装 3.20.0 )

```
下载地址：
https://github.com/erlang/rebar3/archive/refs/tags/3.20.0.tar.gz 

解压：  
tar -xvf  rebar3-3.20.0.tar.gz 

进入解压目录
cd rebar3-3.20.0

修改 rebar3/rebar.config,  文件末尾添加 {plugins, [rebar3_hex]}.

编译
./bootstrap
./rebar3 local install

添加到PATH，修改环境变量
vi ~/.bashrc

将以下内容存储至文件中，保存并退出
export PATH=$PATH:~/.cache/rebar3/bin

使环境变量刷新并生效
source ~/.bashrc   

验证版本 
rebar3 -v
rebar3 hex
```


* 编译EMQX Broker

```
拷贝源码：  git clone -b v5.4.0  https://github.com/emqx/emqx.git emqx-v5.4.0
编译执行：  export BUILD_WITHOUT_QUIC=1;make
```



### 下载EMQX的Kafka插件源码并编译

```shell
> 下载Kafka插件的源码： git clone https://github.com/caijinpeng1113/emqx_plugin_kafka.git
> 进入目录： cd emqx_plugin_kafka
> 执行编译Kafka插件的命令： make rel

在对应的源码目录下，生成编译后插件包，如下：
_build/default/emqx_plugrel/emqx_plugin_kafka-<vsn>.tar.gz
```


### 启动EMQX并配置Kafka插件

* 启动EMQX服务，然后将编译后emqx_plugin_kafka插件包，通过EMQX的插件管理页面，进行安装Kafka插件。（先不要启动Kafka插件）
* 在EMQX服务中，检查 emqx-v5.4.0/_build/emqx/rel/emqx/etc/ 目录下，是否存在 emqx_plugin_kafka.hocon 文件（如果此文件不存在，需要新建），配置文件内容如下，然后进行再启动Kafka插件。
  （注意: 检查Kafka的地址和用户密码是否正确，相关topic是否已创建）

```shell
plugin_kafka {
  // required
  connection {
    // Kafka address.
    bootstrap_hosts = ["10.3.64.223:9192", "10.3.64.223:9292", "10.3.64.223:9392"]

    // enum: per_partition | per_broker
    // optional   default:per_partition
    connection_strategy = per_partition
    // optional   default:5s
    min_metadata_refresh_interval = 5s

    sasl {
      // enum:  plain | scram_sha_256 | scram_sha_512
      mechanism = plain
      username = "admin"
      password = "admin"
    }
    ssl {
      enable = false
    }
  }

  // optional
  producer {
    // Most number of bytes to collect into a produce request.
    // optional   default:896KB
    max_batch_bytes = 896KB
    // enum:  no_compression | snappy | gzip
    // optional   default:no_compression
    compression = no_compression
    // enum:  random | roundrobin | first_key_dispatch
    // optional   default:random
    partition_strategy = random

    // enum:  plain | base64
    encode_payload_type = plain
  }

  // create kafka topic [mqtt_data] and [emqx_test]
  hooks = [
    {endpoint = client.connect}
    , {endpoint = client.connack}
    , {endpoint = client.connected, kafka_topic = mqtt_data}
    , {endpoint = client.disconnected, kafka_topic = mqtt_data}
    , {endpoint = client.authenticate}
    , {endpoint = client.authorize}
    , {endpoint = client.authenticate}
    , {endpoint = client.check_authz_complete}
    , {endpoint = session.created}
    , {endpoint = session.subscribed}
    , {endpoint = session.unsubscribed}
    , {endpoint = session.resumed}
    , {endpoint = session.discarded}
    , {endpoint = session.takenover}
    , {endpoint = session.terminated}
    , {endpoint = message.publish, kafka_topic = mqtt_data,  filter = "sys/#"}
    , {endpoint = message.delivered, kafka_topic = mqtt_data, filter = "sys/#"}
    , {endpoint = message.acked, filter = "sys/#"}
    , {endpoint = message.dropped, filter = "sys/#"}
  ]
}

```

Some examples in the directory `priv/example/`.

#### Hook Point

|          endpoint           |  filter  |
| :-------------------------: | :------: |
|       client.connect        |    /     |
|       client.connack        |    /     |
|      client.connected       |    /     |
|     client.disconnected     |    /     |
|     client.authenticate     |    /     |
|      client.authorize       |    /     |
|     client.authenticate     |    /     |
| client.check_authz_complete |    /     |
|       session.created       |    /     |
|     session.subscribed      |    /     |
|    session.unsubscribed     |    /     |
|       session.resumed       |    /     |
|      session.discarded      |    /     |
|      session.takenover      |    /     |
|     session.terminated      |    /     |
|       message.publish       | required |
|      message.delivered      | required |
|        message.acked        | required |
|       message.dropped       | required |

#### Path

- Default path： `emqx/etc/emqx_plugin_kafka.hocon`
- Attach to path:  set system environment variables  `export EMQX_PLUGIN_KAFKA_CONF="absolute_path"`



![1111111](https://github.com/caijinpeng1113/emqx_plugin_kafka/assets/158483689/9cbd8283-a60b-4365-8767-4ec282cc530e)




