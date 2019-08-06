using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaTest {
    public class CreateStreamIdRequest {
        public long RequestId { get; set; }
        public string CameraId { get; set; }
    }
}
