use {
    crate::node::Kind,
    cid::Cid,
    std::{error::Error, vec::Vec},
};

// type Subset struct {
// 	Kind   int
// 	First  int
// 	Last   int
// 	Blocks List__Link
// }
/// Representation of a `Kind::Subset` node referencing a range of blocks.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Subset {
    /// Kind discriminator copied from the CBOR payload.
    pub kind: u64,
    /// First slot included in the subset.
    pub first: u64,
    /// Last slot included in the subset.
    pub last: u64,
    /// Block CIDs covered by this subset.
    pub blocks: Vec<Cid>,
}

impl Subset {
    /// Decodes a [`Subset`] from raw CBOR bytes.
    pub fn from_bytes(data: Vec<u8>) -> Result<Subset, Box<dyn Error>> {
        let decoded_data: serde_cbor::Value = serde_cbor::from_slice(&data).unwrap();
        let subset = Subset::from_cbor(decoded_data)?;
        Ok(subset)
    }

    /// Decodes a [`Subset`] from a CBOR [`serde_cbor::Value`].
    pub fn from_cbor(val: serde_cbor::Value) -> Result<Subset, Box<dyn Error>> {
        let mut subset = Subset {
            kind: 0,
            first: 0,
            last: 0,
            blocks: vec![],
        };

        if let serde_cbor::Value::Array(array) = val {
            // println!("Kind: {:?}", array[0]);
            if let Some(serde_cbor::Value::Integer(kind)) = array.first() {
                // println!("Kind: {:?}", Kind::from_u64(kind as u64).unwrap().to_string());
                subset.kind = *kind as u64;

                if *kind as u64 != Kind::Subset as u64 {
                    return Err(Box::new(std::io::Error::other(std::format!(
                        "Wrong kind for Subset. Expected {:?}, got {:?}",
                        Kind::Subset,
                        kind
                    ))));
                }
            }
            if let Some(serde_cbor::Value::Integer(first)) = array.get(1) {
                subset.first = *first as u64;
            }
            if let Some(serde_cbor::Value::Integer(last)) = array.get(2) {
                subset.last = *last as u64;
            }

            if let Some(serde_cbor::Value::Array(blocks)) = &array.get(3) {
                for block in blocks {
                    if let serde_cbor::Value::Bytes(block) = block {
                        subset
                            .blocks
                            .push(Cid::try_from(block[1..].to_vec()).unwrap());
                    }
                }
            }
        }
        Ok(subset)
    }

    /// Renders the subset as a JSON object for debugging.
    pub fn to_json(&self) -> serde_json::Value {
        let mut blocks = vec![];
        for block in &self.blocks {
            blocks.push(serde_json::json!({
                "/": block.to_string()
            }));
        }

        let mut map = serde_json::Map::new();
        map.insert("kind".to_string(), serde_json::Value::from(self.kind));
        map.insert("first".to_string(), serde_json::Value::from(self.first));
        map.insert("last".to_string(), serde_json::Value::from(self.last));
        map.insert("blocks".to_string(), serde_json::Value::from(blocks));

        serde_json::Value::from(map)
    }
}

#[cfg(test)]
mod subset_tests {
    use super::*;

    #[test]
    fn test_subset() {
        let subset = Subset {
            kind: 3,
            first: 1,
            last: 1,
            blocks: vec![
                Cid::try_from(
                    vec![
                        1, 113, 18, 32, 56, 148, 167, 251, 237, 117, 200, 226, 181, 134, 79, 115,
                        131, 220, 232, 143, 20, 67, 224, 179, 48, 130, 197, 123, 226, 85, 85, 56,
                        38, 84, 106, 225,
                    ]
                    .as_slice(),
                )
                .unwrap(),
            ],
        };
        let json = subset.to_json();

        let wanted_json = serde_json::json!({
            "kind": 3,
            "first": 1,
            "last": 1,
            "blocks": [
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
                132, 3, 26, 1, 1, 20, 132, 26, 1, 1, 147, 122, 153, 0, 10, 216, 42, 88, 37, 0, 1,
                113, 18, 32, 171, 44, 101, 67, 48, 30, 181, 51, 44, 16, 143, 7, 188, 62, 233, 242,
                13, 126, 131, 177, 206, 83, 39, 8, 109, 55, 106, 108, 246, 68, 188, 190, 216, 42,
                88, 37, 0, 1, 113, 18, 32, 41, 103, 178, 93, 163, 133, 3, 197, 246, 123, 174, 32,
                44, 55, 75, 209, 111, 118, 185, 246, 174, 211, 209, 86, 127, 36, 135, 78, 84, 145,
                18, 85, 216, 42, 88, 37, 0, 1, 113, 18, 32, 232, 137, 216, 146, 217, 111, 118, 6,
                4, 157, 25, 149, 50, 252, 180, 133, 70, 107, 252, 167, 184, 118, 54, 192, 17, 117,
                244, 117, 94, 221, 62, 72, 216, 42, 88, 37, 0, 1, 113, 18, 32, 182, 156, 81, 7, 53,
                117, 125, 56, 128, 210, 171, 237, 59, 18, 203, 234, 249, 136, 0, 60, 135, 205, 75,
                201, 136, 124, 98, 31, 247, 190, 79, 178, 216, 42, 88, 37, 0, 1, 113, 18, 32, 74,
                107, 89, 189, 63, 4, 252, 112, 225, 250, 127, 136, 85, 96, 105, 120, 199, 245, 117,
                10, 136, 186, 254, 156, 106, 255, 174, 226, 238, 203, 204, 135, 216, 42, 88, 37, 0,
                1, 113, 18, 32, 214, 127, 219, 231, 172, 145, 78, 16, 140, 203, 97, 22, 73, 107,
                66, 148, 196, 198, 179, 23, 232, 248, 37, 26, 130, 217, 125, 157, 139, 158, 177,
                143, 216, 42, 88, 37, 0, 1, 113, 18, 32, 192, 86, 238, 92, 94, 208, 2, 251, 84, 19,
                151, 100, 51, 250, 211, 147, 58, 175, 70, 95, 60, 121, 151, 175, 210, 229, 75, 79,
                205, 205, 121, 156, 216, 42, 88, 37, 0, 1, 113, 18, 32, 184, 7, 130, 0, 219, 244,
                235, 51, 62, 197, 227, 138, 232, 12, 181, 199, 242, 62, 111, 121, 119, 183, 36,
                163, 252, 199, 123, 146, 181, 45, 244, 246, 216, 42, 88, 37, 0, 1, 113, 18, 32,
                192, 149, 98, 169, 203, 64, 51, 106, 5, 184, 40, 111, 120, 188, 103, 53, 51, 139,
                245, 36, 64, 250, 89, 30, 94, 151, 56, 78, 93, 98, 127, 81, 216, 42, 88, 37, 0, 1,
                113, 18, 32, 99, 41, 78, 195, 237, 220, 74, 85, 77, 26, 11, 77, 20, 156, 11, 188,
                55, 107, 6, 92, 178, 153, 250, 123, 45, 136, 116, 133, 255, 68, 119, 36,
            ];
            let as_json_raw = serde_json::json!({"kind":3,"first":16848004,"last":16880506,"blocks":[{"/":"bafyreiflfrsugma6wuzsyeepa66d52psbv7ihmookmtqq3jxnjwpmrf4xy"},{"/":"bafyreibjm6zf3i4fapc7m65oeawdos6rn53lt5vo2pivm7zeq5hfjeisku"},{"/":"bafyreihirhmjfwlpoydajhizsuzpznefizv7zj5yoy3maelv6r2v5xj6ja"},{"/":"bafyreifwtriqonlvpu4ibuvl5u5rfs7k7geaapehzvf4tcd4mip7ppspwi"},{"/":"bafyreicknnm32pye7ryod6t7rbkwa2lyy72xkcuixl7jy2x7v3ro5s6mq4"},{"/":"bafyreigwp7n6plerjyiizs3bczewwquuytdlgf7i7asrvawzpwoyxhvrr4"},{"/":"bafyreigak3xfyxwqal5vie4xmqz7vu4thkxumxz4pgl27uxfjnh43tlztq"},{"/":"bafyreifya6babw7u5mzt5rpdrluaznoh6i7g66lxw4skh7ghpojlklpu6y"},{"/":"bafyreigasvrkts2agnvalobin54lyzzvgof7kjca7jmr4xuxhbhf2yt7ke"},{"/":"bafyreiddffhmh3o4jjku2gqljukjyc54g5vqmxfsth5hwlmiosc76rdxeq"}]});

            let decoded_data: serde_cbor::Value = serde_cbor::from_slice(&raw).unwrap();
            let subset = Subset::from_cbor(decoded_data).unwrap();
            let json = subset.to_json();

            assert_eq!(json, as_json_raw);
        }
        {
            let raw = vec![
                132, 3, 26, 1, 1, 147, 123, 26, 1, 1, 246, 95, 153, 0, 10, 216, 42, 88, 37, 0, 1,
                113, 18, 32, 223, 228, 23, 242, 157, 150, 112, 152, 198, 153, 5, 80, 134, 58, 177,
                13, 31, 254, 64, 198, 244, 157, 217, 164, 27, 224, 31, 48, 23, 229, 249, 246, 216,
                42, 88, 37, 0, 1, 113, 18, 32, 170, 53, 139, 63, 239, 79, 17, 75, 50, 107, 250,
                202, 10, 114, 197, 236, 166, 204, 212, 82, 212, 202, 167, 38, 147, 121, 218, 10,
                109, 49, 139, 165, 216, 42, 88, 37, 0, 1, 113, 18, 32, 104, 170, 191, 229, 126,
                102, 195, 134, 213, 14, 28, 202, 214, 180, 166, 229, 55, 132, 95, 162, 139, 51, 67,
                64, 150, 153, 29, 135, 49, 60, 102, 210, 216, 42, 88, 37, 0, 1, 113, 18, 32, 147,
                241, 231, 210, 1, 141, 241, 243, 133, 161, 19, 215, 50, 22, 71, 228, 176, 144, 158,
                128, 97, 139, 93, 124, 19, 34, 88, 5, 170, 16, 82, 126, 216, 42, 88, 37, 0, 1, 113,
                18, 32, 192, 160, 241, 127, 94, 75, 241, 105, 177, 72, 216, 237, 143, 237, 80, 177,
                123, 26, 3, 163, 134, 55, 106, 220, 130, 6, 49, 75, 101, 58, 117, 185, 216, 42, 88,
                37, 0, 1, 113, 18, 32, 166, 236, 76, 71, 214, 207, 96, 6, 12, 152, 247, 133, 146,
                66, 134, 106, 60, 110, 55, 68, 158, 146, 183, 39, 119, 61, 169, 202, 220, 21, 138,
                175, 216, 42, 88, 37, 0, 1, 113, 18, 32, 146, 232, 166, 18, 68, 255, 198, 80, 234,
                182, 199, 222, 106, 110, 200, 154, 5, 118, 40, 137, 65, 79, 199, 11, 245, 148, 50,
                50, 146, 196, 11, 167, 216, 42, 88, 37, 0, 1, 113, 18, 32, 111, 158, 159, 7, 9,
                235, 182, 248, 10, 102, 143, 86, 160, 218, 165, 43, 54, 200, 227, 32, 218, 44, 36,
                230, 188, 245, 3, 105, 215, 208, 120, 17, 216, 42, 88, 37, 0, 1, 113, 18, 32, 217,
                14, 77, 61, 142, 65, 240, 89, 184, 245, 27, 16, 35, 37, 181, 40, 142, 86, 229, 219,
                16, 19, 4, 59, 9, 24, 132, 34, 167, 14, 14, 237, 216, 42, 88, 37, 0, 1, 113, 18,
                32, 12, 224, 20, 15, 97, 134, 22, 48, 186, 156, 15, 237, 105, 100, 54, 140, 176,
                70, 65, 237, 83, 95, 224, 201, 163, 83, 99, 226, 196, 143, 240, 63,
            ];
            let as_json_raw = serde_json::json!({"kind":3,"first":16880507,"last":16905823,"blocks":[{"/":"bafyreig74ql7fhmwocmmngifkcddvmind77ebrxutxm2ig7ad4ybpzpz6y"},{"/":"bafyreifkgwft732pcffte272zifhfrpmu3gniuwuzktsne3z3ifg2mmluu"},{"/":"bafyreidivk76k7tgyodnkdq4zllljjxfg6cf7iulgnbubfuzdwdtcpdg2i"},{"/":"bafyreiet6ht5eamn6hzyliit24zbmr7ewcij5adbrnoxyezclac2uecspy"},{"/":"bafyreigaudyx6xsl6fu3csgy5wh62ufrpmnahi4gg5vnzaqggffwkotvxe"},{"/":"bafyreifg5rgepvwpmadazghxqwjefbtkhrxdore6sk3so5z5vhfnyfmkv4"},{"/":"bafyreies5ctberh7yziovnwh3zvg5se2av3crckbj7dqx5mugizjfralu4"},{"/":"bafyreidpt2pqocplw34auzupk2qnvjjlg3eogig2fqsonphvanu5pudyce"},{"/":"bafyreigzbzgt3dsb6bm3r5i3carslnjirzlolwyqcmcdwciyqqrkodqo5u"},{"/":"bafyreiam4aka6ymgcyylvhap5vuwinumwbded3ktl7qmti2tmprmjd7qh4"}]});

            let decoded_data: serde_cbor::Value = serde_cbor::from_slice(&raw).unwrap();
            let subset = Subset::from_cbor(decoded_data).unwrap();
            let json = subset.to_json();

            assert_eq!(json, as_json_raw);
        }
    }
}
