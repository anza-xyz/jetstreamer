use {
    crate::{dataframe, node::Kind, utils::Buffer},
    std::{error::Error, vec::Vec},
};

// type Rewards struct {
// 	Kind int
// 	Slot int
// 	Data DataFrame
// }
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Rewards {
    pub kind: u64,
    pub slot: u64,
    pub data: dataframe::DataFrame,
}

impl Rewards {
    pub fn from_bytes(data: Vec<u8>) -> Result<Rewards, Box<dyn Error>> {
        let decoded_data: serde_cbor::Value = serde_cbor::from_slice(&data).unwrap();
        let rewards = Rewards::from_cbor(decoded_data)?;
        Ok(rewards)
    }

    // from serde_cbor::Value
    pub fn from_cbor(val: serde_cbor::Value) -> Result<Rewards, Box<dyn Error>> {
        let mut rewards = Rewards {
            kind: 0,
            slot: 0,
            data: dataframe::DataFrame {
                kind: 0,
                hash: None,
                index: None,
                total: None,
                data: Buffer::new(),
                next: None,
            },
        };

        if let serde_cbor::Value::Array(array) = val {
            // println!("Kind: {:?}", array[0]);
            if let Some(serde_cbor::Value::Integer(kind)) = array.first() {
                // println!("Kind: {:?}", Kind::from_u64(kind as u64).unwrap().to_string());
                rewards.kind = *kind as u64;

                if *kind as u64 != Kind::Rewards as u64 {
                    return Err(Box::new(std::io::Error::other(std::format!(
                        "Wrong kind for Rewards. Expected {:?}, got {:?}",
                        Kind::Rewards,
                        kind
                    ))));
                }
            }
            if let Some(serde_cbor::Value::Integer(slot)) = array.get(1) {
                rewards.slot = *slot as u64;
            }

            if let Some(serde_cbor::Value::Array(data)) = &array.get(2) {
                rewards.data =
                    dataframe::DataFrame::from_cbor(serde_cbor::Value::Array(data.clone()))?;
            }
        }
        Ok(rewards)
    }

    pub fn to_json(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        map.insert("kind".to_string(), serde_json::Value::from(self.kind));
        map.insert("slot".to_string(), serde_json::Value::from(self.slot));
        map.insert("data".to_string(), self.data.to_json());

        serde_json::Value::from(map)
    }

    /// Returns whether the rewards data is complete or is split into multiple dataframes.
    pub fn is_complete(&self) -> bool {
        self.data.next.is_none() || self.data.next.as_ref().unwrap().is_empty()
    }
}

#[cfg(test)]
mod rewards_tests {
    use {super::*, cid::Cid};

    #[test]
    fn test_rewards() {
        let rewards = Rewards {
            kind: 5,
            slot: 1,
            data: dataframe::DataFrame {
                kind: 6,
                hash: Some(1),
                index: Some(1),
                total: Some(1),
                data: Buffer::from_vec(vec![1]),
                next: Some(vec![
                    Cid::try_from(
                        vec![
                            1, 113, 18, 32, 56, 148, 167, 251, 237, 117, 200, 226, 181, 134, 79,
                            115, 131, 220, 232, 143, 20, 67, 224, 179, 48, 130, 197, 123, 226, 85,
                            85, 56, 38, 84, 106, 225,
                        ]
                        .as_slice(),
                    )
                    .unwrap(),
                ]),
            },
        };
        let json = rewards.to_json();

        let wanted_json = serde_json::json!({
            "kind": 5,
            "slot": 1,
            "data": {
                "kind": 6,
                "hash": "1",
                "index": 1,
                "total": 1,
                "data": Buffer::from_vec(vec![1]).to_string(),
                "next": [
                    {
                        "/":"bafyreibysst7x3lvzdrllbspoob5z2epcrb6bmzqqlcxxysvku4cmvdk4e"
                    }
                ]
            }
        });

        assert_eq!(json, wanted_json);
    }

    #[test]
    fn test_decoding() {
        {
            let raw = vec![
                131, 5, 26, 1, 1, 20, 132, 133, 6, 246, 246, 246, 85, 40, 181, 47, 253, 4, 0, 65,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 187, 27, 219, 202,
            ];
            let as_json_raw = serde_json::json!({"kind":5,"slot":16848004,"data":{"kind":6,"hash":null,"index":null,"total":null,"data":"KLUv/QQAQQAAAAAAAAAAAAC7G9vK","next":null}});

            let rewards = Rewards::from_bytes(raw).unwrap();
            let as_json = rewards.to_json();
            assert_eq!(as_json, as_json_raw);
        }
        {
            let raw = vec![
                131, 5, 26, 1, 1, 20, 132, 133, 6, 246, 246, 246, 85, 40, 181, 47, 253, 4, 0, 65,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 187, 27, 219, 202,
            ];
            let as_json_raw = serde_json::json!({"kind":5,"slot":16848004,"data":{"kind":6,"hash":null,"index":null,"total":null,"data":"KLUv/QQAQQAAAAAAAAAAAAC7G9vK","next":null}});

            let rewards = Rewards::from_bytes(raw).unwrap();
            let as_json = rewards.to_json();
            assert_eq!(as_json, as_json_raw);
        }
    }
}
