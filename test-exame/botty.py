from flask import Flask, render_template, request
import pyttsx3
import datetime
import requests

app = Flask(__name__)

def assistant(text):
    engine = pyttsx3.init()
    voices = engine.getProperty('voices')
    engine.setProperty('voice', voices[1].id)
    engine.say(text)
    engine.runAndWait()
    print("Assistant respond:", text)

def greeting():
    assistant("Hello, I am your Virtual Assistant. How can I help you?")

def get_input_method():
    return "type"  # Always use text input

def get_text_input():
    user_input = input("You (type): ").lower()
    return user_input

def the_day():
    day = datetime.datetime.today().weekday() + 1
    Day_dict = {
        1: 'Monday', 2: 'Tuesday',
        3: 'Wednesday', 4: 'Thursday',
        5: 'Friday', 6: 'Saturday',
        7: 'Sunday'
    }
    if day in Day_dict.keys():
        weekday = Day_dict[day]
        response = "It's " + weekday
        assistant(response)

def the_time():
    current_time = datetime.datetime.now().strftime("%H:%M")
    response = "The current time is " + current_time
    assistant(response)
    
def search_species_by_common_name(common_name):
    api_url = "https://perenual.com/api/species-list?key=sk-Dlgl651fb53c424f52363"

    try:
        response = requests.get(api_url)
        data = response.json()

        if "data" in data:
            species_list = data["data"]

            matching_species = []

            for species in species_list:
                if "common_name" in species and common_name.lower() in species["common_name"].lower():
                    matching_species.append(species)

            return matching_species

        else:
            print("No data found in the API response.")
            return []

    except requests.exceptions.RequestException as e:
        print("An error occurred while fetching data from the API:", str(e))
        return []

@app.route('/')
def index():
    return render_template('index.html')
@app.route('/response', methods=['POST'])
def response():
    user_input = request.form['user_input']
    input_method = "type"  # You can modify this to handle different input methods in the future

    if "what is your name" in user_input:
        response = "I am your nameless virtual assistant"
        assistant(response)

    elif "bye" in user_input:
        response = "Exiting. Have a Good Day"
        assistant(response)

    elif "what day is it" in user_input:
        the_day()

    elif "what time is it" in user_input:
        the_time()

    elif "tree" in user_input:
        response = "Please enter the common name of the species you want to search for:"
        assistant(response)
        common_name_input = request.form['common_name_input']
        matching_species = search_species_by_common_name(common_name_input)
        if matching_species:
            # Prepare the response to send back to the web page
            response = []
            for index, species in enumerate(matching_species[:5]):
                response.append({
                    "Common Name": species.get('common_name', 'N/A'),
                    "Scientific Name": species.get('scientific_name', 'N/A'),
                    "Other Name": species.get('other_name', 'N/A'),
                    "Cycle": species.get('cycle', 'N/A'),
                    "Watering": species.get('watering', 'N/A'),
                    "Sunlight": species.get('sunlight', 'N/A'),
                    "Image": species.get('default_image', 'N/A'),
                })
        else:
            response = "No matching species found."

        return render_template('index.html', user_input=user_input, response=response)

if __name__ == '__main__':
    app.run(debug=True)