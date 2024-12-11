use {
    crate::core::{
        node::Kind,
        utils::{self, Hash},
    },
    cid::Cid,
    std::{error::Error, vec::Vec},
};

// type Entry struct {
// 	Kind         int
// 	NumHashes    int
// 	Hash         []uint8
// 	Transactions List__Link
// }
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Entry {
    pub kind: u64,
    pub num_hashes: u64,
    pub hash: Hash,
    pub transactions: Vec<Cid>,
}

impl Entry {
    pub fn from_bytes(data: Vec<u8>) -> Result<Entry, Box<dyn Error>> {
        let decoded_data: serde_cbor::Value = serde_cbor::from_slice(&data).unwrap();
        let entry = Entry::from_cbor(decoded_data)?;
        Ok(entry)
    }

    // from serde_cbor::Value
    pub fn from_cbor(val: serde_cbor::Value) -> Result<Entry, Box<dyn Error>> {
        let mut entry = Entry {
            kind: 0,
            num_hashes: 0,
            hash: utils::Hash(vec![]),
            transactions: vec![],
        };

        if let serde_cbor::Value::Array(array) = val {
            // println!("Kind: {:?}", array[0]);
            if let Some(serde_cbor::Value::Integer(kind)) = array.first() {
                // println!("Kind: {:?}", Kind::from_u64(kind as u64).unwrap().to_string());
                entry.kind = *kind as u64;

                if *kind as u64 != Kind::Entry as u64 {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        std::format!(
                            "Wrong kind for Entry. Expected {:?}, got {:?}",
                            Kind::Entry,
                            kind
                        ),
                    )));
                }
            }
            if let Some(serde_cbor::Value::Integer(num_hashes)) = array.get(1) {
                entry.num_hashes = *num_hashes as u64;
            }
            if let Some(serde_cbor::Value::Bytes(hash)) = &array.get(2) {
                entry.hash = Hash(hash.to_vec());
            }

            if let Some(serde_cbor::Value::Array(transactions)) = &array.get(3) {
                for transaction in transactions {
                    if let serde_cbor::Value::Bytes(transaction) = transaction {
                        entry
                            .transactions
                            .push(Cid::try_from(transaction[1..].to_vec()).unwrap());
                    }
                }
            }
        }
        Ok(entry)
    }

    pub fn to_json(&self) -> serde_json::Value {
        let mut transactions = vec![];
        for transaction in &self.transactions {
            transactions.push(serde_json::json!({
                "/": transaction.to_string()
            }));
        }

        let mut map = serde_json::Map::new();
        map.insert("kind".to_string(), serde_json::Value::from(self.kind));
        map.insert(
            "num_hashes".to_string(),
            serde_json::Value::from(self.num_hashes),
        );
        map.insert(
            "hash".to_string(),
            serde_json::Value::from(self.hash.clone().to_string()),
        );
        if self.transactions.is_empty() {
            map.insert("transactions".to_string(), serde_json::Value::Null);
        } else {
            map.insert(
                "transactions".to_string(),
                serde_json::Value::from(transactions),
            );
        }

        serde_json::Value::from(map)
    }
}

#[cfg(test)]
mod entry_tests {
    use super::*;

    #[test]
    fn test_link() {
        let _cid = Cid::try_from(
            vec![
                1, 113, 18, 32, 56, 148, 167, 251, 237, 117, 200, 226, 181, 134, 79, 115, 131, 220,
                232, 143, 20, 67, 224, 179, 48, 130, 197, 123, 226, 85, 85, 56, 38, 84, 106, 225,
            ]
            .as_slice(),
        )
        .unwrap();
        println!("Link: {:?}", _cid);
        // base58 must be bafyreibysst7x3lvzdrllbspoob5z2epcrb6bmzqqlcxxysvku4cmvdk4e
        assert_eq!(
            _cid.to_string(),
            "bafyreibysst7x3lvzdrllbspoob5z2epcrb6bmzqqlcxxysvku4cmvdk4e"
        );
    }

    #[test]
    fn test_entry() {
        let entry = Entry {
            kind: 1,
            num_hashes: 1,
            hash: Hash::from_vec(vec![
                56, 148, 167, 251, 237, 117, 200, 226, 181, 134, 79, 115, 131, 220, 232, 143, 20,
                67, 224, 179, 48, 130, 197, 123, 226, 85, 85, 56, 38, 84, 106, 225,
            ]),
            transactions: vec![Cid::try_from(
                vec![
                    1, 113, 18, 32, 56, 148, 167, 251, 237, 117, 200, 226, 181, 134, 79, 115, 131,
                    220, 232, 143, 20, 67, 224, 179, 48, 130, 197, 123, 226, 85, 85, 56, 38, 84,
                    106, 225,
                ]
                .as_slice(),
            )
            .unwrap()],
        };
        let json = entry.to_json();

        let wanted_json = serde_json::json!({
            "kind": 1,
            "num_hashes": 1,
            "hash": Hash::from_vec(vec![
                56, 148, 167, 251, 237, 117, 200, 226, 181, 134, 79, 115, 131, 220, 232, 143, 20,
                67, 224, 179, 48, 130, 197, 123, 226, 85, 85, 56, 38, 84, 106, 225,
            ]),
            "transactions": [
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
                132, 1, 25, 48, 212, 88, 32, 58, 67, 205, 130, 225, 64, 135, 55, 64, 253, 233, 36,
                218, 65, 37, 172, 48, 226, 254, 197, 235, 146, 52, 77, 187, 43, 180, 119, 105, 115,
                254, 236, 128,
            ];
            let as_json_raw = serde_json::json!({"kind":1,"num_hashes":12500,"hash":"3a43cd82e140873740fde924da4125ac30e2fec5eb92344dbb2bb4776973feec","transactions":null});

            let entry = Entry::from_bytes(raw).unwrap();
            let as_json = entry.to_json();
            assert_eq!(as_json, as_json_raw);
        }
        {
            let raw = vec![
                132, 1, 25, 48, 212, 88, 32, 177, 44, 50, 78, 85, 251, 134, 28, 230, 239, 13, 49,
                94, 211, 17, 91, 234, 82, 246, 190, 200, 60, 240, 156, 152, 114, 199, 13, 230, 159,
                223, 234, 128,
            ];
            let as_json_raw = serde_json::json!({"kind":1,"num_hashes":12500,"hash":"b12c324e55fb861ce6ef0d315ed3115bea52f6bec83cf09c9872c70de69fdfea","transactions":null});

            let entry = Entry::from_bytes(raw).unwrap();
            let as_json = entry.to_json();
            assert_eq!(as_json, as_json_raw);
        }
        {
            let raw = vec![
                132, 1, 25, 48, 212, 88, 32, 71, 92, 57, 208, 67, 29, 20, 121, 163, 95, 163, 73,
                158, 10, 141, 214, 228, 114, 37, 79, 95, 115, 68, 8, 168, 150, 169, 253, 165, 33,
                153, 149, 128,
            ];
            let as_json_raw = serde_json::json!({"kind":1,"num_hashes":12500,"hash":"475c39d0431d1479a35fa3499e0a8dd6e472254f5f734408a896a9fda5219995","transactions":null});

            let entry = Entry::from_bytes(raw).unwrap();
            let as_json = entry.to_json();
            assert_eq!(as_json, as_json_raw);
        }
        {
            let raw = vec![
                132, 1, 25, 47, 147, 88, 32, 135, 179, 249, 90, 215, 133, 165, 232, 199, 181, 255,
                174, 68, 179, 124, 32, 12, 39, 213, 70, 72, 112, 84, 84, 137, 86, 12, 33, 122, 72,
                215, 152, 129, 216, 42, 88, 37, 0, 1, 113, 18, 32, 56, 148, 167, 251, 237, 117,
                200, 226, 181, 134, 79, 115, 131, 220, 232, 143, 20, 67, 224, 179, 48, 130, 197,
                123, 226, 85, 85, 56, 38, 84, 106, 225,
            ];
            let as_json_raw = serde_json::json!({"kind":1,"num_hashes":12179,"hash":"87b3f95ad785a5e8c7b5ffae44b37c200c27d5464870545489560c217a48d798","transactions":[{"/":"bafyreibysst7x3lvzdrllbspoob5z2epcrb6bmzqqlcxxysvku4cmvdk4e"}]});

            let entry = Entry::from_bytes(raw).unwrap();
            let as_json = entry.to_json();
            assert_eq!(as_json, as_json_raw);
        }
    }
}
