using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaTest {
    public class CreateStreamIdResult {
        public string CameraId { get; set; }
        public string StreamId { get; set; }
        public long RequestId { get; set; }
    }
}
