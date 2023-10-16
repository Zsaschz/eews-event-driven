import React, { useRef, useEffect, useState } from "react";
import "../style.css";
import "bootstrap/dist/css/bootstrap.min.css";
import { divIcon } from "leaflet";
import L from "leaflet";

const Map = (props) => {
  const [stations, setStations] = useState([]);
  const [predictions, setPredictions] = useState([]);
  const mapRef = useRef();
  let lst = useRef([]);

  function customMarkerIcon(
    station,
    iconAnchor = [25, 41],
    popupAnchor = [-11, -40]
  ) {
    return divIcon({
      html: `<div class="marker-container"><img class="my-div-img" src="https://i.ibb.co/Ms1qfS3/red-marker.png"/><span class="my-div-span">${station}</span></div>`,
      iconAnchor: iconAnchor,
      popupAnchor: popupAnchor,
    });
  }

  function popupContent(station, description) {
    return `${station} <br /> ${description} <br />`;
  }

  function setSource(stasiun, koord, mag, depth) {
    var magnitude = Math.round((mag + Number.EPSILON) * 100) / 100;
    var depths = Math.round((depth + Number.EPSILON) * 100) / 100;
    let text = `Koordinat: ${koord} Magnitude: ${magnitude} Kedalaman: ${depths} KM Stasiun Pendeteksi: ${stasiun}`;
    var circle = new L.circle(koord, 50000, {
      color: "red",
      opacity: 0.5,
    }).bindPopup(text);
    mapRef.current.addLayer(circle);
    setPredictions((prev) => [...prev, circle]);
  }

  useEffect(() => {
    async function getStations() {
      const response = await fetch(`http://${props.url}/station`);
      const jsonData = await response.json();
      setStations(jsonData);
      jsonData.forEach((element) => {
        L.marker(
          [
            element["location"]["coordinates"][1], // latitude
            element["location"]["coordinates"][0], // longitude
          ],
          { icon: customMarkerIcon(element["name"]) }
        )
          .addTo(mapRef.current)
          .bindPopup(popupContent(element["name"], element["description"]));
      });
    }
    if (mapRef.current == undefined) {
      mapRef.current = L.map("map", {
        layers: [
          L.tileLayer(
            "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
          ),
        ],
        zoomControl: false,
      }).setView([-8.0219, 111.8], 6.5);

      L.control
        .zoom({
          position: "topright",
        })
        .addTo(mapRef.current);
    }
    getStations();
  }, []);

  useEffect(() => {
    predictions.forEach((value) => {
      mapRef.current.removeLayer(value);
    });
    setPredictions([]);

    props.prediction.forEach((value) => {
      setSource(
        value["station"],
        [value["lat"], value["long"]],
        value["magnitude"],
        value["depth"]
      );
    });
  }, [props]);

  return <div id="map"></div>;
};
export default Map;
