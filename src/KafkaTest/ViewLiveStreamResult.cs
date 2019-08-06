using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaTest {
    public class ViewLiveStreamResult {
        public long RequestId { get; set; }
        public string CameraId { get; set; }
        public string ClientId { get; set; }
        public string StreamId { get; set; }
        public bool IsSuccess { get; set; }
        public string Error { get; set; }
        public override string ToString()
        {
            return $"{CameraId},{ClientId},{IsSuccess},{Error}";
        }
    }
}
