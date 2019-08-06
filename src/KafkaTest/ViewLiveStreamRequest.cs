using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaTest {
    public class ViewLiveStreamRequest {
        public long RequestId { get; set; }
        public string CameraId {get; set;}
        public string ClientId { get; set; }

        public ViewLiveStreamRequest()
        {
            RequestId = DateTime.Now.Ticks;
        }
        public override string ToString()
        {
            return $"{CameraId},{ClientId}";
        }
    }
}
