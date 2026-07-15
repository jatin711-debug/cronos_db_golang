import { describe, it, expect } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { MemoryRouter, Routes, Route } from "react-router-dom";
import { MobileNav } from "@/components/MobileNav";

describe("MobileNav", () => {
  it("opens and closes the menu", () => {
    render(
      <MemoryRouter>
        <MobileNav />
      </MemoryRouter>,
    );

    const button = screen.getByLabelText("Toggle menu");
    expect(button).toBeInTheDocument();

    // Menu hidden initially
    expect(screen.queryByText("Dashboard")).not.toBeInTheDocument();

    fireEvent.click(button);
    expect(screen.getByText("Dashboard")).toBeInTheDocument();
    expect(screen.getByText("Cluster")).toBeInTheDocument();

    fireEvent.click(button);
    expect(screen.queryByText("Dashboard")).not.toBeInTheDocument();
  });

  it("navigates and closes menu on link click", () => {
    render(
      <MemoryRouter initialEntries={["/dashboard"]}>
        <Routes>
          <Route path="/dashboard" element={<MobileNav />} />
          <Route path="/cluster" element={<div>Cluster Page</div>} />
        </Routes>
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByLabelText("Toggle menu"));
    fireEvent.click(screen.getByText("Cluster"));

    expect(screen.getByText("Cluster Page")).toBeInTheDocument();
    expect(screen.queryByText("Dashboard")).not.toBeInTheDocument();
  });
});
