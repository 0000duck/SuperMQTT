using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Disconnecting;
using MQTTnet.Client.Options;
using MQTTnet.Client.Publishing;
using MQTTnet.Client.Receiving;
using MQTTnet.Client.Unsubscribing;
using MQTTnet.Protocol;

using System;
using System.Linq;
using System.Threading.Tasks;

namespace SuperMQTT
{
    /// <summary>
    /// 连接MQTT的类
    /// </summary>
    public class Mqtt
    {
        /// <summary>
        /// 构造方法
        /// </summary>
        public Mqtt() { }

        /// <summary>
        /// 构造方法
        /// </summary>
        /// <param name="hostName">MQTT主机</param>
        /// <param name="port">MQTT端口</param>
        /// <param name="userName">MQTT用户名</param>
        /// <param name="password">MQTT密码</param>
        public Mqtt(string hostName, int port, string userName, string password)
        {
            HostName = hostName;
            Port = port;
            UserName = userName;
            Password = password;
        }

        private IMqttClient mqttClient;

        private IMqttClientOptions options;

        /// <summary>
        /// 异常事件
        /// </summary>
        public event Action<Exception> OnException = null;

        /// <summary>
        /// 接收到消息事件
        /// </summary>
        public event Action<string, byte[]> OnReceive = null;

        /// <summary>
        /// 连接关闭事件
        /// </summary>
        public event Action<Exception> OnDisconnected = null;

        /// <summary>
        /// MQTT主机
        /// </summary>
        public string HostName { get; set; }

        /// <summary>
        /// MQTT端口
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        /// MQTT用户名
        /// </summary>
        public string UserName { get; set; }

        /// <summary>
        /// MQTT密码
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// MQTT连接状态
        /// </summary>
        public bool IsConnected { get { return mqttClient != null && mqttClient.IsConnected; } }

        /// <summary>
        /// 连接到MQTT
        /// </summary>
        /// <param name="host"></param>
        /// <param name="port"></param>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public bool Connect()
        {
            try
            {
                if (mqttClient == null)
                {
                    mqttClient = new MqttFactory().CreateMqttClient();
                    mqttClient.DisconnectedHandler = new MqttClientDisconnectedHandlerDelegate(arg => Task.Run(() =>
                    {
                        try
                        {
                            OnDisconnected?.Invoke(arg.Exception);
                        }
                        catch (Exception ex)
                        {
                            OnException?.Invoke(ex);
                        }
                    }));
                    mqttClient.ApplicationMessageReceivedHandler = new MqttApplicationMessageReceivedHandlerDelegate(obj =>
                    {
                        try
                        {
                            OnReceive?.Invoke(obj.ApplicationMessage.Topic, obj.ApplicationMessage.Payload);
                        }
                        catch (Exception ex)
                        {
                            OnException?.Invoke(ex);
                        }
                    });
                }
                if (!mqttClient.IsConnected)
                {
                    options = new MqttClientOptionsBuilder()    //实例化一个MqttClientOptionsBulider
                   .WithTcpServer(HostName, Port)
                   .WithCredentials(UserName, Password)
                   .WithClientId(Guid.NewGuid().ToString())
                   .Build();
                    var result = mqttClient.ConnectAsync(options).Result;
                }
                return mqttClient.IsConnected;
            }
            catch (Exception ex)
            {
                OnException?.Invoke(ex);
                return false;
            }
        }

        /// <summary>
        /// 断开连接
        /// </summary>
        public void Disconnect()
        {
            try
            {
                mqttClient.DisconnectAsync().Wait();
            }
            catch (Exception ex)
            {
                OnException?.Invoke(ex);
            }
        }

        /// <summary>
        /// 发布消息
        /// </summary>
        /// <param name="topic">通道</param>
        /// <param name="message">消息</param>
        /// <param name="qosLevel">0：最多一次，1：至少一次，2：有且仅有一次</param>
        /// <param name="retain"></param>
        /// <returns></returns>
        public bool Publish(string topic, byte[] message, byte qosLevel = 0, bool retain = false)
        {
            try
            {
                if (!Connect())
                {
                    return false;
                }
                var mamb = new MqttApplicationMessageBuilder()
                    .WithTopic(topic)
                    .WithPayload(message).WithRetainFlag(retain);
                if (qosLevel == 0)
                {
                    mamb = mamb.WithAtMostOnceQoS();
                }
                else if (qosLevel == 1)
                {
                    mamb = mamb.WithAtLeastOnceQoS();
                }
                else if (qosLevel == 2)
                {
                    mamb = mamb.WithExactlyOnceQoS();
                }
                var result = mqttClient.PublishAsync(mamb.Build()).Result;
                return result.ReasonCode == MqttClientPublishReasonCode.Success;
            }
            catch (Exception ex)
            {
                OnException?.Invoke(ex);
                return false;
            }
        }

        /// <summary>
        /// 订阅消息
        /// </summary>
        /// <param name="topic"></param>
        /// <returns></returns>
        public bool Subscribe(params string[] topic)
        {
            try
            {
                var result = mqttClient.SubscribeAsync(topic.ToList().ConvertAll(item => new MqttTopicFilter() { Topic = item, QualityOfServiceLevel = MqttQualityOfServiceLevel.AtMostOnce }).ToArray()).Result;
                return result.Items.All(item => (int)item.ResultCode <= 2);
            }
            catch (Exception ex)
            {
                OnException?.Invoke(ex);
                return false;
            }
        }

        /// <summary>
        /// 取消订阅
        /// </summary>
        /// <param name="topic"></param>
        /// <returns></returns>
        public bool UnSubscribe(params string[] topic)
        {
            try
            {
                var result = mqttClient.UnsubscribeAsync(topic).Result;
                return result.Items.All(item => item.ReasonCode == MqttClientUnsubscribeResultCode.Success);
            }
            catch (Exception ex)
            {
                OnException?.Invoke(ex);
                return false;
            }
        }
    }
}
