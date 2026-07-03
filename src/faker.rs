// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};

use crate::context::Context;
use crate::rng::Rng;

const STREET_TYPES: &[&str] = &[
    "Rue", "Avenue", "Boulevard", "Place", "Chemin", "Route",
    "All\u{00e9}e", "Passage", "Square", "Cit\u{00e9}", "Lotissement",
    "R\u{00e9}sidence", "Zone", "Impasse", "Venelle", "Ruelle",
    "Cour", "Domaine", "Hameau", "Lieu-dit", "Villa",
    "Traverse", "Esplanade", "Promenade", "Quai", "Port",
    "Berges", "Sentier",
];

const STREET_NAMES: &[&str] = &[
    "de la R\u{00e9}publique", "Victor Hugo", "de la Paix", "Nationale",
    "Charles de Gaulle", "Jean Jaur\u{00e8}s", "Pasteur", "Gambetta",
    "de la Gare", "des \u{00c9}coles", "du Ch\u{00e2}teau", "de l'\u{00c9}glise",
    "du Moulin", "de la Libert\u{00e9}", "de Verdun", "Lafayette",
    "Foch", "L\u{00e9}on Blum", "de la R\u{00e9}sistance", "des Tilleuls",
    "du G\u{00e9}n\u{00e9}ral de Gaulle", "Jean Moulin", "Louis Pasteur",
    "Anatole France", "Voltaire", "Moli\u{00e8}re", "Hugo", "Balzac",
    "Zola", "Flaubert", "Romain Rolland", "Camus", "Sartre",
    "Proust", "de la Fontaine", "des Roses", "du Midi",
    "de la For\u{00ea}t", "des Acacias", "Carnot", "Joffre",
    "Cl\u{00e9}menceau", "Poincar\u{00e9}", "Mitterrand", "Napol\u{00e9}on",
    "des Capucines", "du Lac", "de la Vall\u{00e9}e", "de la Montagne",
    "des Pr\u{00e9}s", "du Stade", "de l'Industrie", "de l'Avenir",
    "des Pins", "des Ch\u{00ea}nes", "de la Croix", "Saint-Nicolas",
    "Saint-Pierre", "Saint-Paul", "du Commerce", "des Artisans",
    "de la Chapelle", "du Bois", "du Marais", "des Lilas",
    "des Vignes", "du Moulin Neuf", "de la Source",
    "des Hirondelles", "du Vieux Moulin", "de la Rivi\u{00e8}re",
    "des Charmes", "du Parc", "de Bretagne", "d'Alsace",
    "de Provence", "des Alpes", "de la Plaine", "du Soleil",
    "de l'Esp\u{00e9}rance", "Jaur\u{00e8}s", "Briand", "Clemenceau",
    "de Lattre-de-Tassigny", "Leclerc", "de Gaulle", "Gallieni",
    "P\u{00e9}tain", "Pershing", "Roosevelt", "de la Mairie",
    "du Commerce", "du Port", "du Moulin", "de la Poste",
    "du March\u{00e9}", "des \u{00c9}coles", "des Arts", "des Fleurs",
    "des Ormes", "des Fr\u{00ea}nes", "des Marronniers",
    "des Peupliers", "des Platanes", "des Saules",
    "des \u{00c9}rables", "des H\u{00ea}tres", "du 8 Mai 1945",
    "du 11 Novembre", "du 14 Juillet", "du 1er Mai",
    "Franklin Roosevelt", "Winston Churchill", "John Kennedy",
    "Nelson Mandela", "Martin Luther King", "Mahatma Gandhi",
    "Albert Einstein", "Marie Curie", "Pierre Curie",
    "Claude Bernard", "Denis Papin", "Blaise Pascal",
    "Gustave Eiffel", "Ferdinand de Lesseps", "Edmond Rostand",
    "Marcel Proust", "Honor\u{00e9} de Balzac", "\u{00c9}mile Zola",
    "Alexandre Dumas", "Gustave Flaubert", "Moli\u{00e8}re",
    "Racine", "Corneille", "Brillat-Savarin", "Hector Berlioz",
    "Claude Debussy", "Maurice Ravel", "Georges Bizet",
    "Jacques Offenbach", "Camille Saint-Sa\u{00eb}ns",
    "Gabriel Faur\u{00e9}", "Francis Poulenc", "Darius Milhaud",
    "Olivier Messiaen", "Pierre Boulez",
];

fn choose<'a, T>(rng: &mut Rng, slice: &'a [T]) -> Option<&'a T> {
    if slice.is_empty() {
        None
    } else {
        Some(&slice[rng.next_usize(slice.len())])
    }
}

/// Generate a single French address + postal code.
fn generate_one(rng: &mut Rng, cities_json: &serde_json::Value) -> (String, String) {
    let region = cities_json
        .as_object()
        .and_then(|map| {
            let mut keys: Vec<&str> = map.keys().map(|s| s.as_str()).collect();
            keys.sort_unstable();
            choose(rng, &keys).copied()
        })
        .unwrap_or("ile_de_france");
    let city_data = cities_json[region]
        .as_array()
        .and_then(|arr| choose(rng, arr))
        .and_then(|v| v.as_array())
        .expect("invalid city data");
    let city = city_data[0].as_str().unwrap_or("Paris").to_string();
    let postal = city_data[1].as_str().unwrap_or("75001").to_string();
    let street_number: u32 = rng.gen_range(1, 200);
    let street_type = choose(rng, STREET_TYPES).unwrap_or(&"Rue");
    let street_name = choose(rng, STREET_NAMES).unwrap_or(&"de la République");
    let address = format!("{street_number} {street_type} {street_name}, {city}");
    (address, postal)
}

/// Generate `n` French addresses + postal codes using the french_cities nested pool.
pub fn generate_french_addresses(
    ctx: &Context,
    n: usize,
    seed: u64,
) -> Result<(ArrayRef, ArrayRef), String> {
    let cities_json = ctx
        .pool_store
        .get_nested("french_cities")
        .ok_or_else(|| "french_cities pool not loaded".to_string())?;

    let mut rng = Rng::new(seed);
    let mut addresses = Vec::with_capacity(n);
    let mut postals = Vec::with_capacity(n);

    for _ in 0..n {
        let (addr, postal) = generate_one(&mut rng, cities_json);
        addresses.push(addr);
        postals.push(postal);
    }

    Ok((Arc::new(StringArray::from(addresses)), Arc::new(StringArray::from(postals))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::Context;

    fn test_ctx() -> Context {
        let pools_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("dupehell/assets/pools");
        Context::new("kyc", pools_dir.to_str().unwrap()).unwrap()
    }

    #[test]
    fn test_generate_french_addresses_basic() {
        let ctx = test_ctx();
        let (addresses, postals) = generate_french_addresses(&ctx, 10, 42).unwrap();
        assert_eq!(addresses.len(), 10);
        assert_eq!(postals.len(), 10);
        use arrow::array::AsArray;
        let addr_arr = addresses.as_string::<i32>();
        let postal_arr = postals.as_string::<i32>();
        for i in 0..10 {
            let addr = addr_arr.value(i);
            let postal = postal_arr.value(i);
            assert!(!addr.is_empty(), "address {i} should not be empty");
            assert!(!postal.is_empty(), "postal {i} should not be empty");
            assert!(addr.contains(", "), "address {i} should contain ', ' separator: '{addr}'");
        }
    }

    #[test]
    fn test_generate_french_addresses_deterministic() {
        let ctx = test_ctx();
        let (a1, p1) = generate_french_addresses(&ctx, 5, 42).unwrap();
        let (a2, p2) = generate_french_addresses(&ctx, 5, 42).unwrap();
        use arrow::array::AsArray;
        let a1_arr = a1.as_string::<i32>();
        let a2_arr = a2.as_string::<i32>();
        let p1_arr = p1.as_string::<i32>();
        let p2_arr = p2.as_string::<i32>();
        for i in 0..5 {
            assert_eq!(a1_arr.value(i), a2_arr.value(i), "address mismatch at {i}");
            assert_eq!(p1_arr.value(i), p2_arr.value(i), "postal mismatch at {i}");
        }
    }

    #[test]
    fn test_generate_french_addresses_diff_seeds() {
        let ctx = test_ctx();
        let (a1, _) = generate_french_addresses(&ctx, 10, 42).unwrap();
        let (a2, _) = generate_french_addresses(&ctx, 10, 99).unwrap();
        use arrow::array::AsArray;
        let a1_arr = a1.as_string::<i32>();
        let a2_arr = a2.as_string::<i32>();
        let mut same = 0;
        for i in 0..10 {
            if a1_arr.value(i) == a2_arr.value(i) {
                same += 1;
            }
        }
        assert!(same < 5, "different seeds should produce different results");
    }
}
