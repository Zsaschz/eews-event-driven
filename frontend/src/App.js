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
  var api = process.env.NODE_ENV == 'production'? process.env.REACT_APP_API : 'localhost';
  return (
    <Router>
      <Navibar />
      <Routes>
        <Route exact path="/" element={<AdminMap url={api} />} />
        <Route path="/about" element={<About />} />
        <Route
          path="/admin-map"
          element={<AdminMap url={api} />}
        />
        <Route path="/login" element={<LogIn />} />
        <Route path="/help" element={<Help />} />
      </Routes>
    </Router>
  );
};
export default App;
