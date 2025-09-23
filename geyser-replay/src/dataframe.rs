use {
    crate::{node::Kind, utils::Buffer},
    cid::Cid,
    std::{error::Error, vec::Vec},
};

// type DataFrame struct {
// 	Kind  int
// 	Hash  **int
// 	Index **int
// 	Total **int
// 	Data  []uint8
// 	Next  **List__Link
// }
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct DataFrame {
    pub kind: u64,
    pub hash: Option<u64>,
    pub index: Option<u64>,
    pub total: Option<u64>,
    pub data: Buffer,
    pub next: Option<Vec<Cid>>,
}

impl DataFrame {
    pub fn from_bytes(data: Vec<u8>) -> Result<DataFrame, Box<dyn Error>> {
        let decoded_data: serde_cbor::Value = serde_cbor::from_slice(&data).unwrap();
        let data_frame = DataFrame::from_cbor(decoded_data)?;
        Ok(data_frame)
    }
    // from serde_cbor::Value
    pub fn from_cbor(val: serde_cbor::Value) -> Result<DataFrame, Box<dyn Error>> {
        let mut data_frame = DataFrame {
            kind: 0,
            hash: None,
            index: None,
            total: None,
            data: Buffer::new(),
            next: None,
        };

        if let serde_cbor::Value::Array(array) = val {
            // println!("Kind: {:?}", array[0]);
            if let Some(serde_cbor::Value::Integer(kind)) = array.first() {
                // println!("Kind: {:?}", Kind::from_u64(kind as u64).unwrap().to_string());
                data_frame.kind = *kind as u64;

                if *kind as u64 != Kind::DataFrame as u64 {
                    return Err(Box::new(std::io::Error::other(
                        std::format!(
                            "Wrong kind for DataFrame. Expected {:?}, got {:?}",
                            Kind::DataFrame,
                            kind
                        ),
                    )));
                }
            }
            if let Some(serde_cbor::Value::Integer(hash)) = array.get(1) {
                data_frame.hash = Some(*hash as u64);
            }
            if let Some(serde_cbor::Value::Integer(index)) = array.get(2) {
                data_frame.index = Some(*index as u64);
            }
            if let Some(serde_cbor::Value::Integer(total)) = array.get(3) {
                data_frame.total = Some(*total as u64);
            }
            if let Some(serde_cbor::Value::Bytes(data)) = &array.get(4) {
                data_frame.data = Buffer::from_vec(data.clone());
            }

            if array.len() > 5
                && let Some(serde_cbor::Value::Array(next)) = &array.get(5) {
                    if next.is_empty() {
                        data_frame.next = None;
                    } else {
                        let mut nexts = vec![];
                        for cid in next {
                            if let serde_cbor::Value::Bytes(cid) = cid {
                                nexts.push(Cid::try_from(cid[1..].to_vec()).unwrap());
                            }
                        }
                        data_frame.next = Some(nexts);
                    }
                }
        }
        Ok(data_frame)
    }

    pub fn to_json(&self) -> serde_json::Value {
        let mut next = vec![];
        if let Some(nexts) = &self.next {
            for cid in nexts {
                next.push(serde_json::json!({
                    "/": cid.to_string()
                }));
            }
        }

        let mut map = serde_json::Map::new();
        map.insert("kind".to_string(), serde_json::Value::from(self.kind));
        if self.hash.is_none() {
            map.insert("hash".to_string(), serde_json::Value::Null);
        } else {
            let hash_as_string = self.hash.unwrap().to_string();
            map.insert("hash".to_string(), serde_json::Value::from(hash_as_string));
        }
        if self.index.is_none() {
            map.insert("index".to_string(), serde_json::Value::Null);
        } else {
            map.insert("index".to_string(), serde_json::Value::from(self.index));
        }
        if self.total.is_none() {
            map.insert("total".to_string(), serde_json::Value::Null);
        } else {
            map.insert("total".to_string(), serde_json::Value::from(self.total));
        }
        map.insert(
            "data".to_string(),
            serde_json::Value::from(self.data.to_string()),
        );
        if next.is_empty() {
            map.insert("next".to_string(), serde_json::Value::Null);
        } else {
            map.insert("next".to_string(), serde_json::Value::from(next));
        }

        serde_json::Value::from(map)
    }
}

#[cfg(test)]
mod data_frame_tests {
    use super::*;

    #[test]
    fn test_data_frame() {
        let data_frame = DataFrame {
            kind: 6,
            hash: Some(1),
            index: Some(1),
            total: Some(1),
            data: Buffer::from_vec(vec![1]),
            next: Some(vec![
                Cid::try_from(
                    vec![
                        1, 113, 18, 32, 56, 148, 167, 251, 237, 117, 200, 226, 181, 134, 79, 115,
                        131, 220, 232, 143, 20, 67, 224, 179, 48, 130, 197, 123, 226, 85, 85, 56,
                        38, 84, 106, 225,
                    ]
                    .as_slice(),
                )
                .unwrap(),
            ]),
        };
        let json = data_frame.to_json();

        let wanted_json = serde_json::json!({
            "kind": 6,
            "hash": "1",
            "index": 1,
            "total": 1,
            "data": Buffer::from_vec(vec![1]).to_string(),
            "next": [
               {
                "/": "bafyreibysst7x3lvzdrllbspoob5z2epcrb6bmzqqlcxxysvku4cmvdk4e"
               }
            ]
        });

        assert_eq!(json, wanted_json);
    }

    #[test]
    fn test_decoding() {
        {
            let raw = vec![
                134, 6, 59, 70, 48, 192, 168, 213, 38, 83, 193, 1, 2, 70, 32, 119, 111, 114, 108,
                100, 128,
            ];
            let as_json_raw = serde_json::json!({"kind":6,"hash":"13388989860809387070","index":1,"total":2,"data":"IHdvcmxk","next":null});

            let data_frame = DataFrame::from_bytes(raw).unwrap();
            let as_json = data_frame.to_json();
            assert_eq!(as_json, as_json_raw);
        }
        {
            let raw = vec![
                134, 6, 27, 72, 172, 245, 101, 152, 189, 52, 248, 24, 26, 24, 28, 74, 178, 79, 233,
                101, 240, 6, 201, 17, 9, 14, 128,
            ];
            let as_json_raw = serde_json::json!({"kind":6,"hash":"5236830283428082936","index":26,"total":28,"data":"sk/pZfAGyREJDg==","next":null});

            let data_frame = DataFrame::from_bytes(raw).unwrap();
            let as_json = data_frame.to_json();
            assert_eq!(as_json, as_json_raw);
        }
        {
            let raw = vec![
                134, 6, 27, 72, 172, 245, 101, 152, 189, 52, 248, 22, 24, 28, 74, 111, 237, 179,
                173, 165, 39, 99, 171, 113, 233, 133, 216, 42, 88, 37, 0, 1, 113, 18, 32, 122, 71,
                2, 134, 225, 132, 61, 186, 162, 255, 184, 29, 48, 1, 138, 64, 232, 195, 187, 20, 2,
                107, 96, 133, 253, 99, 212, 159, 214, 235, 31, 176, 216, 42, 88, 37, 0, 1, 113, 18,
                32, 28, 140, 185, 170, 59, 82, 138, 35, 215, 213, 58, 142, 227, 82, 31, 146, 35,
                230, 167, 145, 243, 214, 187, 136, 224, 31, 202, 225, 146, 245, 229, 198, 216, 42,
                88, 37, 0, 1, 113, 18, 32, 107, 199, 31, 114, 114, 251, 65, 56, 222, 108, 243, 54,
                182, 63, 194, 178, 61, 197, 69, 4, 128, 71, 62, 116, 222, 43, 105, 250, 14, 182,
                175, 60, 216, 42, 88, 37, 0, 1, 113, 18, 32, 87, 50, 255, 0, 149, 48, 182, 80, 100,
                55, 160, 92, 192, 112, 136, 95, 186, 77, 166, 159, 244, 11, 211, 12, 111, 235, 187,
                124, 29, 52, 146, 102, 216, 42, 88, 37, 0, 1, 113, 18, 32, 81, 216, 114, 215, 30,
                122, 54, 226, 139, 196, 54, 28, 133, 44, 128, 91, 199, 16, 47, 41, 137, 190, 214,
                97, 150, 108, 65, 242, 217, 51, 49, 79,
            ];
            let as_json_raw = serde_json::json!({"kind":6,"hash":"5236830283428082936","index":22,"total":28,"data":"b+2zraUnY6tx6Q==","next":[{"/":"bafyreid2i4binymehw5kf75yduyadcsa5db3wfacnnqil7ld2sp5n2y7wa"},{"/":"bafyreia4rs42uo2srir5pvj2r3rveh4septkpept225yrya7zlqzf5pfyy"},{"/":"bafyreidly4pxe4x3ie4n43htg23d7qvshxcukbeai47hjxrlnh5a5nvphq"},{"/":"bafyreicxgl7qbfjqwzigin5altahbcc7xjg2nh7ubpjqy37lxn6b2nesmy"},{"/":"bafyreicr3bznoht2g3rixrbwdscszac3y4ic6kmjx3lgdftmihznsmzrj4"}]});

            let data_frame = DataFrame::from_bytes(raw).unwrap();
            let as_json = data_frame.to_json();
            assert_eq!(as_json, as_json_raw);
        }
    }
}
