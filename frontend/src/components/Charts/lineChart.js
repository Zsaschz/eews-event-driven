import { useEffect, useState, useRef } from "react";
import React from "react";
import {
  Line,
  LineChart,
  XAxis,
  YAxis,
  ReferenceLine,
  ResponsiveContainer,
} from "recharts";
import "react-dropdown/style.css";
import "../style.css";

const strokeColor = {
  BHN: "#8884d8",
  BHE: "#ffc658",
  BHZ: "#82ca9d",
};

const RealtimeChart = (props) => {
  const [data, setData] = useState([]);

  useEffect(() => {
    setData(props.data);
  }, [props]);

  return (
    <div data-testid={props.testid} className="responsive-container">
      <p className="station-title">{props.channel}</p>
      <div data-testid={props.testid} className="flex-container">
        <LineChart width={650} height={350} data={data}>
          <XAxis
            dataKey="time"
            xAxisId={0}
            axisLine={true}
            tick={true}
            tickLine={true}
            angle={-45}
            padding={"gap"}
            tickFormatter={(val) =>
              val ? new Date(val).toLocaleTimeString() : ""
            }
          />
          <YAxis type="number" domain={["auto", "auto"]} />
          {Object.values(props.p).map((value) => {
            return (
              <ReferenceLine
                key={`p-${props.stasiun}-${props.channel}-${value}`}
                x={value}
                stroke="red"
              />
            );
          })}
          <Line
            type="linear"
            isAnimationActive={false}
            dataKey="data"
            stroke={strokeColor[props.channel]}
            dot={false}
          />
        </LineChart>
      </div>
    </div>
  );
};
export default RealtimeChart;
