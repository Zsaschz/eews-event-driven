import React, { useState } from "react";
import "./App.css";
import Navibar from "./components/Navbar/Navbar";
import "bootstrap/dist/css/bootstrap.min.css";
import UserMap from "./pages/UserMap";
import AdminMap from "./pages/AdminMap";
import LogIn from "./pages/Login";
import Help from "./pages/Help";
import About from "./pages/About";
import { BrowserRouter as Router, Routes, Route } from "react-router-dom";

const App = () => {
  return (
    <Router>
      <Navibar />
      <Routes>
        <Route exact path="/" element={<AdminMap url={"localhost:8000"} />} />
        <Route path="/about" element={<About />} />
        <Route
          path="/admin-map"
          element={<AdminMap url={"localhost:8000"} />}
        />
        <Route path="/login" element={<LogIn />} />
        <Route path="/help" element={<Help />} />
      </Routes>
    </Router>
  );
};
export default App;
