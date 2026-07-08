def gender_by_first_name(first_name_list):
    import requests, os

    # URL de l'API Genderize.io
    url = "https://api.genderize.io"

    # Résultats
    results = []

    for prenom in first_name_list:
        # Paramètres de la requête
        params = {"name": prenom}

        # Appel à l'API
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            result = {
                "first_name": prenom,
                "gender": data.get("gender", "inconnu"),
                "probability": data.get("probability", 0)
            }
            results.append(result)
        else:
            url = f"https://gender-api.com/get?key={os.environ.get('GENDER_API_KEY')}"

            response = requests.get(url, params=params)

            if response.status_code == 200:
                data = response.json()
                if data.get("result"):
                    result = {
                        "first_name": prenom,
                        "gender": data["result"],
                        "probability": data.get("accuracy", 0)
                    }
                    results.append(result)
                else:
                    print(f"Aucun résultat pour le prénom {prenom}")
            else:
                print(f"Erreur pour le prénom {prenom}: {response.status_code}")

    # Affichage des résultats
    for resultat in results:
        print(f"Prénom: {resultat['first_name']}, Genre: {resultat['gender']}, Probabilité: {resultat['probability']}")

    return results