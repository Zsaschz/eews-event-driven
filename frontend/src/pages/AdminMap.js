import React, { useEffect, useState, useRef, useMemo } from "react";
import "chartjs-plugin-streaming";
import "../App.css";
import RealtimeChart from "../components/Charts/lineChart";
import Ptable from "../components/Tabel";
import MyTime from "../components/Timer/time";
import "bootstrap/dist/css/bootstrap.min.css";
import Map from "../components/Map/Map2";
import Checklist from "../components/Checklist/checklist";
import Legend from "../components/Legend/Legend-Button";
import Button from "react-bootstrap/Button";
import Dropdown from "../components/Charts/dropdown";

const channelOptions = [
  { value: "BHE", label: "BHE" },
  { value: "BHN", label: "BHN" },
  { value: "BHZ", label: "BHZ" },
];

const AdminMap = (props) => {
  const [data, setData] = useState({});
  const [prediction, setPrediction] = useState([]);
  const [showTab, setShowTab] = useState(true);
  const [stationOptions, setStationOptions] = useState(new Set());
  const [stations, setStations] = useState(Object.keys(data));
  const [optionStationSelected, setOptionStationSelected] = useState(stations);
  const [channels, setChannels] = useState(["BHE", "BHN", "BHZ"]);
  const [optionChannelSelected, setOptionChannelSelected] =
    useState(channelOptions);

  function functionMapper(data) {
    let options = [];
    if (data !== undefined) {
      let arr = Array.from(data);
      Object.values(arr).map((station) => {
        options.push({
          value: station,
          label: station,
        });
      });
    }
    return options;
  }

  useEffect(() => {
    const socket = new WebSocket(`ws://${props.url}/ws`);
    socket.onmessage = function (e) {
      const res = JSON.parse(e.data);
      const jsonData = res["data"];
      const prediction = res["prediction"];

      let newOptions = new Set();
      Object.keys(jsonData).forEach((key) => {
        if (!stationOptions.has(key)) {
          newOptions.add(key);
        }
      });

      setData((prev) => {
        let newData = structuredClone(prev);
        Object.entries(jsonData).forEach(([station, value]) => {
          let stationData = newData[station]
            ? newData[station]
            : {
                BHE: Array(100).fill(null),
                BHN: Array(100).fill(null),
                BHZ: Array(100).fill(null),
                p_arrival: [],
              };
          Object.entries(value).forEach(([key, array]) => {
            let arrayData = stationData[key];

            if (key == "p_arrival") {
              Object.values(array).forEach((point) => {
                arrayData.push(new Date(point).getTime());
              });
              stationData[key] = arrayData;
            } else {
              Object.values(array).forEach((point) => {
                point["time"] = new Date(point["time"]).getTime();
                arrayData.push(point);
              });
            }
            stationData[key] = arrayData;
            if (stationData[key].length > array.length * 4) {
              stationData[key] = stationData[key].slice(array.length);
            } else if (stationData[key].length > 200) {
              stationData[key] = stationData[key].slice(50);
            }
            newData[station] = stationData;
          });
        });
        return newData;
      });
      setPrediction(prediction);
      setStationOptions((prev) => new Set([...prev, ...newOptions]));
    };
  }, []);

  return (
    <div>
      <div>
        <Map prediction={prediction} url={props.url} />
        <div className="legend">
          <Legend />
        </div>
        <div className="performance">
          <Button variant="light" onClick={() => setShowTab(!showTab)}>
            <img src={require("../assets/list.png")}></img>
          </Button>
        </div>
        {showTab ? (
          <div className="left-panel">
            <Checklist
              name="station"
              stateChanger={setStations}
              options={functionMapper(stationOptions)}
              setOptionSelected={setOptionStationSelected}
              optionSelected={optionStationSelected}
            />
            <Checklist
              name="channel"
              stateChanger={setChannels}
              options={channelOptions}
              setOptionSelected={setOptionChannelSelected}
              optionSelected={optionChannelSelected}
            />
            <MyTime />
            <div className="chart-container">
              {data != null &&
                Object.entries(data).map(([stasiun, value]) => {
                  if (stations.includes(stasiun)) {
                    return (
                      <div key={stasiun}>
                        <p className="station-title">Stasiun {stasiun}</p>
                        {channels.includes("BHE") && (
                          <RealtimeChart
                            testid={`${stasiun}_BHE`}
                            url={props.url}
                            data={value["BHE"]}
                            p={value["p_arrival"]}
                            stasiun={stasiun}
                            channel="BHE"
                          />
                        )}
                        {channels.includes("BHN") && (
                          <RealtimeChart
                            testid={`${stasiun}_BHN`}
                            url={props.url}
                            data={value["BHN"]}
                            p={value["p_arrival"]}
                            stasiun={stasiun}
                            channel="BHN"
                          />
                        )}
                        {channels.includes("BHZ") && (
                          <RealtimeChart
                            testid={`${stasiun}_BHZ`}
                            url={props.url}
                            data={value["BHZ"]}
                            p={value["p_arrival"]}
                            stasiun={stasiun}
                            channel="BHZ"
                          />
                        )}
                      </div>
                    );
                  }
                })}
            </div>
          </div>
        ) : null}
      </div>
    </div>
  );
};
export default AdminMap;
